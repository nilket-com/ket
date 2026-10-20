use super::profile_three_fixture::{M3_OTHER, M3_PRIME, exact_pin};
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::AdminManifest2;
use crate::storage::mutation_visibility::root2::admin::manifest::AnyAdminManifest2;
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::anchored::{
	AnchorDir2, AnchoredError2,
};
use crate::storage::mutation_visibility::root2::provisioning::inventory::{
	RootIdentity2, RootState2,
};
use crate::storage::mutation_visibility::root2::provisioning::owner_core::{
	EventOp2, OwnerError2, OwnerRequest2, op_id2,
};
use crate::storage::mutation_visibility::root2::provisioning::owner_log::{
	OwnerDir2, OwnerIoError2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::{
	MAX_SNAPSHOT_BYTES2, PinnedBoundTuple2, ReplicaAnchorError2, ReplicaInput2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_read::*;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;
use std::ffi::CString;
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

const G: [u8; 16] = [0xa1; 16];
const C: [u8; 16] = [0xc2; 16];
static NEXT: AtomicUsize = AtomicUsize::new(0);

// A fresh 0700 directory unique to this process and call; removed on drop.
struct Scratch(PathBuf);
impl Scratch {
	fn new() -> Self {
		let n = NEXT.fetch_add(1, Ordering::SeqCst);
		let p = std::env::temp_dir().join(format!("g1c-roundtrip-{}-{n}", std::process::id()));
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
fn sha(b: &[u8]) -> Digest2 {
	crate::storage::mutation_visibility::root2::codec::hash(b"", b)
}
fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn manifest(name: &str) -> (Vec<u8>, Digest2) {
	let a: serde_json::Value =
		serde_json::from_str(include_str!("../provisioning/tests/anchor_vectors.json")).unwrap();
	let m = unhex(a["manifests"][name]["bytes"].as_str().unwrap());
	let pin = unhex(a["manifests"][name]["pin"].as_str().unwrap());
	(m, pin.try_into().unwrap())
}
fn canonical_genesis() -> Digest2 {
	let v: serde_json::Value = serde_json::from_str(include_str!("install_vectors.json")).unwrap();
	unhex(v["genesis"].as_str().unwrap()).try_into().unwrap()
}
fn at(term: u64, node: u32, index: u64) -> EntryPosition2 {
	EntryPosition2 { term, node, index }
}
fn op(n: u8) -> crate::storage::mutation_visibility::root2::provisioning::owner_core::OpId2 {
	op_id2([n; 16]).unwrap()
}
struct Boot {
	control: Digest2,
	length: u64,
	index: u64,
}
impl Boot {
	fn standard() -> Self {
		Self {
			control: manifest("m1").1,
			length: 137,
			index: 2,
		}
	}
	fn pre_root(&self) -> MutationAuthorityState2 {
		let genesis =
			CommittedEntry2::fixture(at(0, 0, 0), canonical_genesis(), EntryClass2::Membership);
		let mut s = advance_pre_root2(&empty_authority2(), &genesis).unwrap().0;
		for i in 1..self.index {
			let scanned =
				CommittedEntry2::fixture(at(1, 1, i), [i as u8; 32], EntryClass2::NonRoot2);
			s = advance_pre_root2(&s, &scanned).unwrap().0;
		}
		s
	}
	// Genesis, scanned entries, then the Bootstrap at (1, 1, index), through the existing fixtures.
	fn rooted(&self) -> MutationAuthorityState2 {
		let cmd = Command2::Bootstrap {
			epoch: 7,
			control: self.control,
			length: self.length,
		};
		let e = TrustedEntry2::fixture(G, C, l(self.index), &cmd).unwrap();
		let (_, _, _, fp, d) = e.fields();
		let verified = VerifiedCommand2::fixture(G, C, &cmd, &[], 0).unwrap();
		let ce = CommittedEntry2::fixture(at(1, 1, self.index), fp, EntryClass2::Bootstrap(d));
		bootstrap2(&self.pre_root(), &ce, &e, &verified, &cmd)
			.unwrap()
			.0
	}
}
fn reserve_m1() -> OwnerRequest2 {
	OwnerRequest2::Reserve {
		manifest: AnyAdminManifest2::One(AdminManifest2::decode(&manifest("m1").0).unwrap()),
	}
}
fn m1_identity() -> RootIdentity2 {
	let (m, pin) = manifest("m1");
	let parsed = AdminManifest2::decode(&m).unwrap();
	RootIdentity2 {
		generation: parsed.generation,
		cluster: parsed.cluster,
		pin,
		admin_key: parsed.admin_key,
	}
}
fn approve(state: &MutationAuthorityState2) -> OwnerRequest2 {
	let id = m1_identity();
	OwnerRequest2::Approve {
		identity: id,
		observation: observe_bootstrap2(state).unwrap(),
		confirmed: id,
	}
}
// Exports into `out` and places the m1 manifest beside it; returns the export's pin, checked here
// independently against the exported bytes.
fn export(owner: &mut OwnerDir2, out: &Path, name: &str) -> Digest2 {
	let pin = owner.export(out, name).unwrap();
	assert_eq!(pin, sha(&fs::read(out.join(name)).unwrap()));
	fs::write(out.join("m1.adm"), manifest("m1").0).unwrap();
	pin
}
fn read(
	dir: &Path,
	manifest_file: &str,
	manifest_pin: Option<&Digest2>,
	snapshot_file: &str,
	inventory_pin: Option<&Digest2>,
) -> Result<PinnedBoundTuple2, ReplicaReadError2> {
	let anchor = AnchorDir2::open(dir).unwrap();
	let m = CString::new(manifest_file).unwrap();
	let s = CString::new(snapshot_file).unwrap();
	read_replica_tuple2(&ReplicaFiles2 {
		anchor: &anchor,
		manifest: &m,
		manifest_pin,
		snapshot: &s,
		inventory_pin,
	})
}
// ---- Profile 3: M3_PRIME (m1's generation, cluster and epoch) in a marked directory ----
fn three_pin() -> Digest2 {
	exact_pin(&M3_PRIME)
}
impl Boot {
	fn three() -> Self {
		Self {
			control: three_pin(),
			length: 241,
			index: 2,
		}
	}
}
fn reserve_three() -> OwnerRequest2 {
	OwnerRequest2::Reserve {
		manifest: AnyAdminManifest2::decode(&M3_PRIME).unwrap(),
	}
}
fn three_identity() -> RootIdentity2 {
	let any = AnyAdminManifest2::decode(&M3_PRIME).unwrap();
	let a = any.admission();
	RootIdentity2 {
		generation: a.generation,
		cluster: a.cluster,
		pin: three_pin(),
		admin_key: a.admin_key,
	}
}
fn approve_three(state: &MutationAuthorityState2) -> OwnerRequest2 {
	let id = three_identity();
	OwnerRequest2::Approve {
		identity: id,
		observation: observe_bootstrap2(state).unwrap(),
		confirmed: id,
	}
}
// Exports into `out` and places the M3_PRIME manifest beside it as "m3.adm".
fn export_three(owner: &mut OwnerDir2, out: &Path, name: &str) -> Digest2 {
	let pin = owner.export(out, name).unwrap();
	assert_eq!(pin, sha(&fs::read(out.join(name)).unwrap()));
	fs::write(out.join("m3.adm"), M3_PRIME).unwrap();
	pin
}
// A marked owner with M3_PRIME reserved and approved from `state`, exported to `out` as "inv".
fn bound_export_three(
	owner_dir: &Scratch,
	out: &Scratch,
	state: &MutationAuthorityState2,
) -> (OwnerDir2, Digest2) {
	let mut owner = OwnerDir2::create_v2(&owner_dir.0).unwrap();
	owner.apply(&op(1), &reserve_three()).unwrap();
	let outcome = owner.apply(&op(2), &approve_three(state)).unwrap();
	assert_eq!(outcome.original.map(|e| e.op), Some(EventOp2::Approve));
	assert_eq!(outcome.current, RootState2::Bound);
	let pin = export_three(&mut owner, &out.0, "inv");
	(owner, pin)
}
// An owner with m1 reserved and approved from `state`, exported to `out` as "inv".
fn bound_export(
	owner_dir: &Scratch,
	out: &Scratch,
	state: &MutationAuthorityState2,
) -> (OwnerDir2, Digest2) {
	let mut owner = OwnerDir2::open(&owner_dir.0).unwrap();
	owner.apply(&op(1), &reserve_m1()).unwrap();
	let outcome = owner.apply(&op(2), &approve(state)).unwrap();
	assert_eq!(outcome.original.map(|e| e.op), Some(EventOp2::Approve));
	assert_eq!(outcome.current, RootState2::Bound);
	let pin = export(&mut owner, &out.0, "inv");
	(owner, pin)
}

#[test]
fn observation_round_trips_through_owner_approval_to_an_installed_admin() {
	let (owner_dir, out) = (Scratch::new(), Scratch::new());
	let state = Boot::three().rooted();
	let observation = observe_bootstrap2(&state).unwrap();
	let (_owner, pin) = bound_export_three(&owner_dir, &out, &state);
	let pin3 = three_pin();
	let tuple = read(&out.0, "m3.adm", Some(&pin3), "inv", Some(&pin)).unwrap();
	let admin = install_admin2(&state, &tuple).unwrap();
	let ctx = admin_context2(&admin);
	let o = observation.identity;
	assert_eq!((ctx.generation, ctx.cluster), (o.generation, o.cluster));
	assert_eq!(ctx.installed_at, o.bootstrap);
	assert_eq!(ctx.bootstrap_fingerprint, o.bootstrap_fingerprint);
	assert_eq!(ctx.bootstrap_command, o.bootstrap_command);
	assert_eq!(ctx.genesis, o.genesis_fingerprint);
	assert_eq!(ctx.initial_epoch, o.initial_epoch);
	assert_eq!(ctx.manifest_pin, o.pin);
	assert_eq!(o.pin, pin3);
	assert_eq!(
		format!(
			"{:?}",
			ReplicaFiles2 {
				anchor: &AnchorDir2::open(&out.0).unwrap(),
				manifest: c"m3.adm",
				manifest_pin: None,
				snapshot: c"inv",
				inventory_pin: None,
			}
		),
		"ReplicaFiles2 { redacted }"
	);
}
#[test]
fn observation_survives_tip_advances() {
	let (owner_dir, out) = (Scratch::new(), Scratch::new());
	let mut state = Boot::three().rooted();
	let original = observe_bootstrap2(&state).unwrap();
	let (_owner, pin) = bound_export_three(&owner_dir, &out, &state);
	let pin3 = three_pin();
	let tuple = read(&out.0, "m3.adm", Some(&pin3), "inv", Some(&pin)).unwrap();
	let tip = |s: &MutationAuthorityState2| {
		let (root, _) = root_and_origin2(s).unwrap();
		(
			root.state.covered,
			root.state.fingerprint,
			root.state.command_digest,
		)
	};
	for (i, authorized) in [(3u64, true), (4, false)] {
		let before = tip(&state);
		let cmd = Command2::Cover {
			kind: 0,
			external: [i as u8; 32],
		};
		let e = TrustedEntry2::fixture(G, C, l(i), &cmd).unwrap();
		let mut verified = VerifiedCommand2::fixture(G, C, &cmd, &[], 0).unwrap();
		if !authorized {
			verified = verified.unauthorized();
		}
		state = advance_root2(&state, &e, &verified, &cmd).unwrap().0;
		assert!(tip(&state) != before, "the tip moved at entry {i}");
		assert!(observe_bootstrap2(&state).unwrap() == original, "entry {i}");
		assert!(install_admin2(&state, &tuple).is_ok(), "entry {i}");
	}
}
#[test]
fn observe_refusals() {
	assert_eq!(
		observe_bootstrap2(&empty_authority2()).err(),
		Some(ObserveError2::PrerequisiteUnavailable)
	);
	assert_eq!(
		observe_bootstrap2(&Boot::three().pre_root()).err(),
		Some(ObserveError2::PrerequisiteUnavailable)
	);
	// A non-241 length on a profile-3 root, placed by the test hook (no Bootstrap roots it).
	let three = Boot::three().rooted();
	assert!(observe_bootstrap2(&three).is_ok());
	for wrong in [240, 242] {
		assert_eq!(
			observe_bootstrap2(&with_control_len_for_test(&three, wrong)).err(),
			Some(ObserveError2::ControlLength),
			"{wrong}"
		);
	}
	// Legacy zero is a valid raw-math class but never an installable manifest.
	let short = Boot {
		length: 0,
		..Boot::standard()
	}
	.rooted();
	assert_eq!(
		observe_bootstrap2(&short).err(),
		Some(ObserveError2::ControlLength)
	);
	// ObserveError2::Codec is defensive and unreachable through accepted transitions: Bootstrap
	// refuses a zero epoch, the fixtures refuse zero generation or cluster, and an invalid Log2
	// never reaches a rooted state. A zero control digest is not refused by the observation codec.
}
#[test]
fn owner_conflict_paths_follow_the_accepted_owner_semantics() {
	let pin3 = three_pin();
	// (a) A differing observation against a Bound root is BoundConflict: refused, no event.
	let (owner_dir, out) = (Scratch::new(), Scratch::new());
	let state = Boot::three().rooted();
	let (mut owner, _) = bound_export_three(&owner_dir, &out, &state);
	let other = Boot {
		index: 3,
		..Boot::three()
	}
	.rooted();
	assert_eq!(
		owner.apply(&op(3), &approve_three(&other)).err(),
		Some(OwnerIoError2::Decision(OwnerError2::BoundConflict))
	);
	let pin = export_three(&mut owner, &out.0, "inv-b");
	let tuple = read(&out.0, "m3.adm", Some(&pin3), "inv-b", Some(&pin)).unwrap();
	assert!(install_admin2(&state, &tuple).is_ok());
	assert_eq!(
		install_admin2(&other, &tuple).err(),
		Some(InstallError2::Record(InstallField2::Bootstrap))
	);
	// (b) A Prepared root given a non-binding observation records a Conflict event.
	let (owner_dir, out) = (Scratch::new(), Scratch::new());
	let mut owner = OwnerDir2::create_v2(&owner_dir.0).unwrap();
	owner.apply(&op(1), &reserve_three()).unwrap();
	let foreign = Boot {
		control: exact_pin(&M3_OTHER),
		..Boot::three()
	}
	.rooted();
	let outcome = owner.apply(&op(2), &approve_three(&foreign)).unwrap();
	assert_eq!(outcome.original.map(|e| e.op), Some(EventOp2::Conflict));
	assert_eq!(outcome.current, RootState2::Conflicted);
	let pin = export_three(&mut owner, &out.0, "inv");
	assert_eq!(
		read(&out.0, "m3.adm", Some(&pin3), "inv", Some(&pin)).err(),
		Some(ReplicaReadError2::Tuple(ReplicaAnchorError2::NotBound(
			RootState2::Conflicted
		)))
	);
}
#[test]
fn retirement_and_pins() {
	let (owner_dir, out) = (Scratch::new(), Scratch::new());
	let state = Boot::standard().rooted();
	let (mut owner, p1) = bound_export(&owner_dir, &out, &state);
	owner
		.apply(
			&op(3),
			&OwnerRequest2::Retire {
				terminal: None,
				identity: m1_identity(),
			},
		)
		.unwrap();
	let p2 = export(&mut owner, &out.0, "inv-2");
	let m1_pin = manifest("m1").1;
	assert_eq!(
		read(&out.0, "m1.adm", Some(&m1_pin), "inv-2", Some(&p2)).err(),
		Some(ReplicaReadError2::Tuple(ReplicaAnchorError2::NotBound(
			RootState2::Retired
		)))
	);
	// Retained limitation: the coherent older Bound export with its own matching pin still
	// validates. This is not a freshness or rollback defence.
	assert!(read(&out.0, "m1.adm", Some(&m1_pin), "inv", Some(&p1)).is_ok());
	assert_eq!(
		read(&out.0, "m1.adm", Some(&m1_pin), "inv-2", Some(&p1)).err(),
		Some(ReplicaReadError2::Tuple(
			ReplicaAnchorError2::InventoryPinMismatch
		))
	);
}
#[test]
fn read_refusals_are_not_normalized() {
	let (owner_dir, out) = (Scratch::new(), Scratch::new());
	let state = Boot::three().rooted();
	let (_owner, pin) = bound_export_three(&owner_dir, &out, &state);
	let (manifest3, pin3) = (M3_PRIME.to_vec(), three_pin());
	let dir = &out.0;
	let tuple = |e: ReplicaAnchorError2| Some(ReplicaReadError2::Tuple(e));
	let read_err = |input, error| Some(ReplicaReadError2::Read { input, error });
	// Pins first: a missing pin outranks a missing file.
	assert_eq!(
		read(dir, "nope", None, "inv", Some(&pin)).err(),
		tuple(ReplicaAnchorError2::PrerequisiteUnavailable(
			ReplicaInput2::ManifestPin
		))
	);
	assert_eq!(
		read(dir, "m3.adm", Some(&pin3), "nope", None).err(),
		tuple(ReplicaAnchorError2::PrerequisiteUnavailable(
			ReplicaInput2::InventoryPin
		))
	);
	assert_eq!(
		read(dir, "nope", Some(&pin3), "inv", Some(&pin)).err(),
		tuple(ReplicaAnchorError2::PrerequisiteUnavailable(
			ReplicaInput2::Manifest
		))
	);
	assert_eq!(
		read(dir, "m3.adm", Some(&pin3), "nope", Some(&pin)).err(),
		tuple(ReplicaAnchorError2::PrerequisiteUnavailable(
			ReplicaInput2::Snapshot
		))
	);
	// Reader refusals are reported as themselves.
	symlink(dir.join("m3.adm"), dir.join("link")).unwrap();
	assert_eq!(
		read(dir, "link", Some(&pin3), "inv", Some(&pin)).err(),
		read_err(ReplicaInput2::Manifest, AnchoredError2::Symlink)
	);
	fs::create_dir(dir.join("adir")).unwrap();
	assert_eq!(
		read(dir, "adir", Some(&pin3), "inv", Some(&pin)).err(),
		read_err(ReplicaInput2::Manifest, AnchoredError2::NotRegular)
	);
	assert_eq!(
		read(dir, "../x", Some(&pin3), "inv", Some(&pin)).err(),
		read_err(ReplicaInput2::Manifest, AnchoredError2::Escape)
	);
	fs::write(dir.join("locked"), &manifest3).unwrap();
	fs::set_permissions(dir.join("locked"), fs::Permissions::from_mode(0o000)).unwrap();
	if fs::read(dir.join("locked")).is_ok() {
		eprintln!("G1C_SKIP permission: running with privilege that ignores mode 000");
	} else {
		assert_eq!(
			read(dir, "locked", Some(&pin3), "inv", Some(&pin)).err(),
			read_err(ReplicaInput2::Manifest, AnchoredError2::Permission)
		);
	}
	// Exact admitted limits: the manifest at 241 bytes is read (the round trip above); one more
	// byte is the reader's TooLarge, reported as Part A's Length.
	fs::write(dir.join("long.adm"), [manifest3.as_slice(), &[0]].concat()).unwrap();
	assert_eq!(
		read(dir, "long.adm", Some(&pin3), "inv", Some(&pin)).err(),
		tuple(ReplicaAnchorError2::Length(ReplicaInput2::Manifest))
	);
	// A short manifest reaches Part A, whose length check precedes its digest check.
	fs::write(dir.join("short.adm"), &manifest3[..10]).unwrap();
	assert_eq!(
		read(dir, "short.adm", Some(&pin3), "inv", Some(&pin)).err(),
		tuple(ReplicaAnchorError2::Length(ReplicaInput2::Manifest))
	);
	// A snapshot of exactly the admitted size passes the reader and, pinned with the hash of those
	// exact bytes, reaches the snapshot codec; one more byte is Length.
	let exact = vec![0u8; MAX_SNAPSHOT_BYTES2];
	fs::write(dir.join("exact"), &exact).unwrap();
	assert!(matches!(
		read(dir, "m3.adm", Some(&pin3), "exact", Some(&sha(&exact))).err(),
		Some(ReplicaReadError2::Tuple(
			ReplicaAnchorError2::SnapshotCodec(_)
		))
	));
	let over = vec![0u8; MAX_SNAPSHOT_BYTES2 + 1];
	fs::write(dir.join("over"), &over).unwrap();
	assert_eq!(
		read(dir, "m3.adm", Some(&pin3), "over", Some(&sha(&over))).err(),
		tuple(ReplicaAnchorError2::Length(ReplicaInput2::Snapshot))
	);
	// A binding refusal passes through: other with its correct pin against the manifest3 inventory.
	let (other, other_pin) = (M3_OTHER.to_vec(), exact_pin(&M3_OTHER));
	fs::write(dir.join("other.adm"), &other).unwrap();
	assert_eq!(
		read(dir, "other.adm", Some(&other_pin), "inv", Some(&pin)).err(),
		tuple(ReplicaAnchorError2::NotInInventory)
	);
}
