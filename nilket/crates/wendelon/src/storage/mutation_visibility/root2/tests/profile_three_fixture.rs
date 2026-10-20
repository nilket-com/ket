//! Neutral profile-3 fixtures for the root2 tests: the manifests every profile-3 suite builds on,
//! the frozen S1 command and claim bytes those suites read, and the generic FIXTURE chain helpers
//! (moved unchanged from the profile-2 binding tests). TEST-ONLY: every key here is derived from a
//! published seed and authorizes nothing outside tests.
//!
//! - `M3_PRIMARY`: the `BASE_B` admission fields with admin `key(11)`, governance
//!   `[0xd2; 16]`/`key(12)` and recovery `[0xd3; 16]`/`key(13)`.
//! - `M3_SIBLING`: `M3_PRIMARY` with recovery `key(14)`, so the same admission fields and a
//!   different pin.
//! - `M3_OTHER`: a distinct root (the `OTHER_*` constants).
//! - `M3_S1`: the S1 roots' generation, cluster and epoch, the S1 claim's namespace, and the
//!   `statement_seeds` keys.
//! - `M3_KAT` / `m3_kat_foreign()`: the auth KAT root's identity and claim namespace with the
//!   `statement_seeds` keys, on the KAT cluster (byte-identical to `M3_S1`) or the foreign cluster.
use super::statement_seeds::{ADMIN_SEED, GOVERNANCE_SEED, RECOVERY_SEED};
use super::*;
use crate::storage::mutation_visibility::MutationScope;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AdminManifest2, AdminManifestThree3, AdminNamespace2, AnyAdminManifest2, GovernanceRole2,
	RecoveryRole3, admin_manifest_digest2, admin_manifest_two_digest2,
};
use crate::storage::mutation_visibility::root2::admin::record::BootstrapRecord2;
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::admin_statement::*;
use crate::storage::mutation_visibility::root2::committed::{CommittedSource2, committed_entry2};
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::inventory::{
	LogState2, RootEntry2, RootIdentity2, RootState2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::encode_snapshot2;
use crate::storage::mutation_visibility::root2::recovery::Deployment2;
use crate::storage::mutation_visibility::root2::tests::governance_verification::support::{
	E, Fixture, carrier, entry, tuple_for,
};
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;
use crate::storage::mutation_visibility::root2::types::{AuthorityRef2, NodeRef2};
use crate::storage::mutation_visibility::root2::verification::advance_verified2;
use ed25519_dalek::{Signer, SigningKey};

mod moves;
mod prime;

const fn nibble(c: u8) -> u8 {
	match c {
		b'0'..=b'9' => c - b'0',
		b'a'..=b'f' => c - b'a' + 10,
		_ => panic!("lower-case hex only"),
	}
}
/// Exact lower-case hex to bytes, at compile time.
const fn bytes_of<const N: usize>(hex: &str) -> [u8; N] {
	let h = hex.as_bytes();
	assert!(h.len() == 2 * N, "hex length");
	let mut out = [0u8; N];
	let mut i = 0;
	while i < N {
		out[i] = (nibble(h[2 * i]) << 4) | nibble(h[2 * i + 1]);
		i += 1;
	}
	out
}
fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
/// The Ed25519 public key of a 32-byte TEST seed.
pub(super) fn public(seed: &[u8; 32]) -> Digest2 {
	SigningKey::from_bytes(seed).verifying_key().to_bytes()
}
/// The Ed25519 public key of the TEST seed `[seed; 32]`.
pub(super) fn key(seed: u8) -> Digest2 {
	public(&[seed; 32])
}

/// Bytes 12..137 of the M2 profile-2 manifest "b", parsed by field (admin seed 0x42).
pub(super) const BASE_B: AdminManifest2 = AdminManifest2 {
	generation: [0x42; 16],
	cluster: [0x4d; 16],
	initial_epoch: 322,
	admin_id: [0x71; 16],
	admin_key: bytes_of("2152f8d19b791d24453242e15f2eab6cb7cffa7b6a5ed30097960e069881db12"),
	namespace: AdminNamespace2 {
		tenant: 5,
		scope: MutationScope::Raw,
		table: [0; 16],
		schema: 0,
	},
};
/// The `M3_PRIMARY` role ids (admin, governance, recovery) and signing seeds.
pub(super) const PRIMARY_GOVERNANCE_ID: [u8; 16] = [0xd2; 16];
pub(super) const PRIMARY_RECOVERY_ID: [u8; 16] = [0xd3; 16];
pub(super) const PRIMARY_SEEDS: [u8; 3] = [11, 12, 13];
/// The `M3_SIBLING` recovery seed; every other field is `M3_PRIMARY`'s.
pub(super) const SIBLING_RECOVERY_SEED: u8 = 14;
/// `M3_OTHER`: a root distinct from `M3_PRIMARY` in generation, cluster, epoch, tenant and every
/// role id and key. Raw scope, table 0, schema 0; role keys from the TEST seeds 31, 32 and 33.
pub(super) const OTHER_GENERATION: [u8; 16] = [0x61; 16];
pub(super) const OTHER_CLUSTER: [u8; 16] = [0x62; 16];
pub(super) const OTHER_INITIAL_EPOCH: u64 = 9;
pub(super) const OTHER_TENANT: u64 = 2;
pub(super) const OTHER_ROLE_IDS: [[u8; 16]; 3] = [[0x63; 16], [0x64; 16], [0x65; 16]];
pub(super) const OTHER_SEEDS: [u8; 3] = [31, 32, 33];
/// `M3_S1`: the generation, cluster and initial epoch of every S1 `root_*` golden, and the S1
/// claim's namespace (tenant 1, Raw, table 0, schema 0).
pub(super) const S1_GENERATION: [u8; 16] = [0x11; 16];
pub(super) const S1_CLUSTER: [u8; 16] = [0x12; 16];
pub(super) const S1_INITIAL_EPOCH: u64 = 1;
pub(super) const S1_TENANT: u64 = 1;
/// The role ids (admin, governance, recovery) of the manifests keyed by `statement_seeds`.
pub(super) const SEEDED_ROLE_IDS: [[u8; 16]; 3] = [[0x51; 16], [0x52; 16], [0x53; 16]];
/// The foreign cluster of the KAT root-mismatch test.
pub(super) const KAT_FOREIGN_CLUSTER: [u8; 16] = [0x34; 16];
/// `M3_PRIME`: the profile-1 G1d fixture manifest's admission fields (generation `0xa1`, cluster
/// `0xc2`, epoch 7, tenant 0, Table `0x7a`, schema 2) with the TEST admin seed 0x5A, governance
/// id 0x6C / seed 0x6B and recovery id 0x7c / seed 0x7b. Every vector bound to that fixture ports
/// to it.
pub(super) const PRIME_ADMIN_SEED: u8 = 0x5A;
pub(super) const PRIME_GOVERNANCE: ([u8; 16], u8) = ([0x6C; 16], 0x6B);
pub(super) const PRIME_RECOVERY: ([u8; 16], u8) = ([0x7c; 16], 0x7b);

pub(super) const M3_PRIMARY: [u8; 241] = bytes_of(concat!(
	"56495341444d320001000300424242424242424242424242424242424d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d",
	"42010000000000007171717171717171717171717171717166be7e332c7a453332bd9d0a7f7db055f5c5ef1a",
	"06ada66d98b39fb6810c473a0300000005000000000000000000000000000000000000000000000000000000",
	"0000000000d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d20b513ad9b4924015ca0902ed079044d3ac5dbec2306f06",
	"948c10da8eb6e39f2dff010000d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d391a28a0b74381593a4d94695792089",
	"26afc8ad82c8839b7644359b9eba9a4b3a1f000000",
));
pub(super) const M3_SIBLING: [u8; 241] = bytes_of(concat!(
	"56495341444d320001000300424242424242424242424242424242424d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d",
	"42010000000000007171717171717171717171717171717166be7e332c7a453332bd9d0a7f7db055f5c5ef1a",
	"06ada66d98b39fb6810c473a0300000005000000000000000000000000000000000000000000000000000000",
	"0000000000d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d20b513ad9b4924015ca0902ed079044d3ac5dbec2306f06",
	"948c10da8eb6e39f2dff010000d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d30beef5a9e679e6a3e134fe27837bff",
	"32c7cb5f5d44ea09bcb0e542bad6a4c0cc1f000000",
));
pub(super) const M3_OTHER: [u8; 241] = bytes_of(concat!(
	"56495341444d3200010003006161616161616161616161616161616162626262626262626262626262626262",
	"09000000000000006363636363636363636363636363636343046bfe4092b3e94994eada15dcc20d8aaa07b6",
	"58fd3954eb8e0efb8bdca5de0300000002000000000000000000000000000000000000000000000000000000",
	"0000000000646464646464646464646464646464644ed32f63bf35f0eeefcb25f28a2e1fbdc873ae2835671b",
	"0c9460f5f12e4556a8ff01000065656565656565656565656565656565884b8857f4eaa1613c61504db34d4b",
	"eaf346517a0e31de3cddd4d9b4201d9d0b1f000000",
));
pub(super) const M3_S1: [u8; 241] = bytes_of(concat!(
	"56495341444d3200010003001111111111111111111111111111111112121212121212121212121212121212",
	"01000000000000005151515151515151515151515151515125fc32c478a5a4f84e4557a0342e08278c883641",
	"b6149bdb0b4a986ab6c2cb780300000001000000000000000000000000000000000000000000000000000000",
	"000000000052525252525252525252525252525252c91cb3ce2b84e4ba85f562ece41edfe4e27afc52d88d50",
	"7f66a18638df823e9fff010000535353535353535353535353535353534dd9b6496a27571acb089a5e3482df",
	"c86acdeddc4d1f28e15a9b04f026d7f2261f000000",
));

pub(super) const M3_PRIME: [u8; 241] = bytes_of(concat!(
	"56495341444d320001000300a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2",
	"07000000000000000e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0d7550754e0800a5d237eef5826035766b9b3e5a",
	"15868a940ab289958788e3b0030000000000000000000000017a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a020000",
	"00000000006c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c8320a51977d8c38ca8a4927c670df5821e449761945e15",
	"e9efb26a1509d230eaff0100007c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7f747b67bd3fe63c2a736739dfe401",
	"56d622347346e70f68f51c178a75ce55371f000000",
));

/// A profile-3 manifest's canonical bytes from its parts, through the published constructors.
pub(super) fn build_three(
	base: AdminManifest2,
	governance: ([u8; 16], Digest2),
	recovery: ([u8; 16], Digest2),
) -> [u8; 241] {
	AdminManifestThree3::new(
		base,
		GovernanceRole2::new(governance.0, governance.1).unwrap(),
		RecoveryRole3::new(recovery.0, recovery.1).unwrap(),
	)
	.unwrap()
	.encode()
	.unwrap()
}
fn kat(field: &str) -> Vec<u8> {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("../auth/tests/vectors.json")).unwrap();
	unhex(v[field].as_str().unwrap())
}
/// The auth KAT root's manifest on `cluster`: generation `kat("generation")`, initial epoch the
/// `claim_c2` original lease epoch, the `claim_c2` namespace, and the `statement_seeds` keys.
pub(super) fn kat_manifest(cluster: [u8; 16]) -> [u8; 241] {
	let claim = ClaimProjection2::decode(&kat("claim_c2")).unwrap();
	let id = claim.manifest.identity;
	let [admin, governance, recovery] = SEEDED_ROLE_IDS;
	build_three(
		AdminManifest2 {
			generation: kat("generation").try_into().unwrap(),
			cluster,
			initial_epoch: claim.original.lease.epoch,
			admin_id: admin,
			admin_key: public(&ADMIN_SEED),
			namespace: AdminNamespace2 {
				tenant: id.tenant,
				scope: id.scope,
				table: id.table,
				schema: id.schema_generation,
			},
		},
		(governance, public(&GOVERNANCE_SEED)),
		(recovery, public(&RECOVERY_SEED)),
	)
}
/// The auth KAT cluster.
pub(super) fn kat_cluster() -> [u8; 16] {
	kat("cluster").try_into().unwrap()
}
/// `M3_KAT`: the KAT root on the KAT cluster. The KAT shares the S1 generation, cluster, epoch 1
/// and claim namespace, so it is byte-identical to `M3_S1` (asserted against `kat_manifest`).
pub(super) const M3_KAT: [u8; 241] = M3_S1;
/// `M3_KAT_FOREIGN`: the KAT root on `KAT_FOREIGN_CLUSTER`.
pub(super) fn m3_kat_foreign() -> [u8; 241] {
	kat_manifest(KAT_FOREIGN_CLUSTER)
}

// ---- The S1 bytes the profile-3 suites read, frozen at the plan commit ----

const S1_FROZEN: [(&str, &str); 5] = [
	(
		"claim_projection2",
		concat!(
			"0200d40000000100111111111111111111111111111111110100000000000000000000000000000000000000",
			"0000000000000000000000000022222222222222222222222222222222010000000000000001000000000000",
			"0001000000333333333333333333333333333333330100000004010001000000000000000000000000006300",
			"0000000000000000000000000000000100000000004444444444444444444444444444444444444444444444",
			"444444444444444444555555555555555555555555555555555555555555555555555555555555555500a1a1",
			"a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a19999999999999999999999999999",
			"9999777777777777777777777777777777770100000000000000010000000000000000000000630000000000",
			"00000a000000000000000000000001000000e803000000000000000000000100000001000000000000000100",
			"0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			"0000000000000000000000000000000000000000000001000000000000000100000004000000000000000000",
			"0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			"0000000000000000000000000000000000000000000000a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2",
			"a2a2a2a2a2a2a2a2a2a2a2016666666666666666666666666666666601000000000000000100000000000000",
			"0000140000000000000000000000010000000100420000000000000001030100000077777777777777777777",
			"77777777777702000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb03000000cccccccccccccccccccccccccccc",
			"cccc0700000000000000a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a30000",
			"0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			"000000000000000000000000000000000000",
		),
	),
	(
		"command_bootstrap",
		concat!(
			"5649534556543032020001005100000011111111111111111111111111111111121212121212121212121212",
			"12121212000100000000000000e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8",
			"550000000000000000de505062e25fd8a8ef379e22df0d91cfce51ea4493ccfc3d3d65df30800528ab",
		),
	),
	(
		"command_cover_blank",
		concat!(
			"5649534556543032020001004200000011111111111111111111111111111111121212121212121212121212",
			"12121212010001010101010101010101010101010101010101010101010101010101010101017c443b79553d",
			"77f254f725718b3b233146d4a09f1cd1056fae2a35d0ab8414ee",
		),
	),
	(
		"command_reserve",
		concat!(
			"5649534556543032020001002303000011111111111111111111111111111111121212121212121212121212",
			"1212121208fe0200000200d40000000100111111111111111111111111111111110100000000000000000000",
			"0000000000000000000000000000000000000000000022222222222222222222222222222222010000000000",
			"0000010000000000000001000000333333333333333333333333333333330100000004010001000000000000",
			"0000000000000063000000000000000000000000000000000100000000004444444444444444444444444444",
			"4444444444444444444444444444444444445555555555555555555555555555555555555555555555555555",
			"55555555555500a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a19999999999",
			"9999999999999999999999777777777777777777777777777777770100000000000000010000000000000000",
			"00000063000000000000000a000000000000000000000001000000e803000000000000000000000100000001",
			"0000000000000001000000000000000000000000000000000000000000000000000000000000000000000000",
			"0000000000000000000000000000000000000000000000000000000000000001000000000000000100000004",
			"0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			"0000000000000000000000000000000000000000000000000000000000000000a2a2a2a2a2a2a2a2a2a2a2a2",
			"a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2016666666666666666666666666666666601000000000000",
			"0001000000000000000000140000000000000000000000010000000100420000000000000001030100000077",
			"77777777777777777777777777777702000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb03000000cccccccccc",
			"cccccccccccccccccccccc0700000000000000a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3",
			"a3a3a3a3a3a3a300000000000000000000000000000000000000000000000000000000000000000000000000",
			"0000000000000000000000000000000000000000000000000000000c6d514f154e3a2a51bdb6f72e9464a40f",
			"dbecfa6979e4340804817c17032358",
		),
	),
	(
		"command_finalize",
		concat!(
			"564953455654303202000100c301000011111111111111111111111111111111121212121212121212121212",
			"1212121209111111111111111111111111111111110100000000000000000000000000000000000000000000",
			"00000000000000000000222222222222222222222222222222220100000000000000b53fdec061b11f97b287",
			"fea380f1d474ecd0670c05b2436f3c72edf9dc2d631401000000000000000100000005000000000000002101",
			"00000100999999999999999999999999999999990100000000000000010000000400000000000000924f0804",
			"cc56b4a5919a6a87343140342efd8662cfa57c3fa3ccf6724856d6d901000000000000000100000005000000",
			"00000000b53fdec061b11f97b287fea380f1d474ecd0670c05b2436f3c72edf9dc2d63146666666666666666",
			"66666666666666660100000000000000010000000000dddddddddddddddddddddddddddddddddddddddddddd",
			"dddddddddddddddddddd02010000007777777777777777777777777777777702000000bbbbbbbbbbbbbbbbbb",
			"bbbbbbbbbbbbbb00000000000000000000000000000000000000000000000000000000000000000000000000",
			"000000000000000000000000000000000000000000000000000000948441e9753b129beb98a53685bdd20932",
			"dc189a4dac983cb55f51e8426c7d65",
		),
	),
];
/// The frozen plan-commit bytes of one S1 golden the profile-3 suites read.
pub(super) fn s1_frozen(name: &str) -> Vec<u8> {
	let (_, hex) = S1_FROZEN
		.iter()
		.find(|(n, _)| *n == name)
		.unwrap_or_else(|| panic!("no frozen S1 entry {name}"));
	unhex(hex)
}
/// A frozen S1 command.
pub(super) fn s1_command(name: &str) -> Command2 {
	Command2::decode(&s1_frozen(name)).unwrap().2
}
/// The frozen S1 claim.
pub(super) fn s1_claim() -> ClaimProjection2 {
	ClaimProjection2::decode(&s1_frozen("claim_projection2")).unwrap()
}

// ---- Generic FIXTURE chain helpers ----

fn hash(b: &[u8]) -> Digest2 {
	crate::storage::mutation_visibility::root2::codec::hash(b"", b)
}
pub(super) fn genesis() -> Digest2 {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("statement_vectors.json")).unwrap();
	unhex(v["genesis"].as_str().unwrap()).try_into().unwrap()
}
fn at(term: u64, node: u32, index: u64) -> EntryPosition2 {
	EntryPosition2 { term, node, index }
}
/// The digest over the exact supplied manifest bytes, by length.
pub(super) fn exact_pin(bytes: &[u8]) -> Digest2 {
	match bytes.len() {
		137 => admin_manifest_digest2(bytes.try_into().unwrap()),
		189 => admin_manifest_two_digest2(bytes.try_into().unwrap()),
		241 => crate::storage::mutation_visibility::root2::codec::hash(
			b"VISROOT2-ADMIN-MANIFEST\0",
			bytes,
		),
		n => panic!("no canonical length {n}"),
	}
}
/// A coherent Part A tuple for this exact profile-3 manifest whose single Bound record is `record`,
/// over the v3 snapshot; Part A must accept it.
pub(super) fn tuple_of(manifest: &[u8], record: BootstrapRecord2) -> PinnedBoundTuple2 {
	let any = AnyAdminManifest2::decode(manifest).unwrap();
	let a = any.admission();
	let pin = exact_pin(manifest);
	let snapshot = encode_snapshot2(&LogState2 {
		roots: vec![RootEntry2 {
			terminal: None,
			identity: RootIdentity2 {
				generation: a.generation,
				cluster: a.cluster,
				pin,
				admin_key: a.admin_key,
			},
			manifest: any.clone(),
			state: RootState2::Bound,
			last_seq: record.inventory_seq,
			record: Some(record),
		}],
		last_seq: record.inventory_seq,
	})
	.unwrap();
	validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(manifest),
		manifest_pin: Some(&pin),
		snapshot: Some(&snapshot),
		inventory_pin: Some(&hash(&snapshot)),
	})
	.expect("Part A accepts the coherent tuple")
}
pub(super) fn pre_root() -> MutationAuthorityState2 {
	let first = CommittedEntry2::fixture(at(0, 0, 0), genesis(), EntryClass2::Membership);
	let s = advance_pre_root2(&empty_authority2(), &first).unwrap().0;
	let one = CommittedEntry2::fixture(at(1, 1, 1), [1; 32], EntryClass2::NonRoot2);
	advance_pre_root2(&s, &one).unwrap().0
}
/// One coherent chain: a FIXTURE-rooted state, its observation, the owner-approved record, the
/// snapshot, its pins and the validated Bound tuple, all derived from this exact command.
pub(super) struct Chain {
	pub any: AnyAdminManifest2,
	pub cmd: Command2,
	pub entry: TrustedEntry2,
	pub committed: CommittedEntry2,
	pub rooted: MutationAuthorityState2,
	pub tuple: PinnedBoundTuple2,
}
/// FIXTURE: root `pre_root` with this Bootstrap through the test-only always-authorized token.
pub(super) fn rooted(
	bytes: &[u8],
	length: u64,
) -> (
	Command2,
	TrustedEntry2,
	CommittedEntry2,
	MutationAuthorityState2,
) {
	let m = AnyAdminManifest2::decode(bytes).unwrap();
	let a = m.admission();
	let cmd = Command2::Bootstrap {
		epoch: a.initial_epoch,
		control: exact_pin(bytes),
		length,
	};
	let e = TrustedEntry2::fixture(a.generation, a.cluster, l(2), &cmd).unwrap();
	let (_, _, _, fp, d) = e.fields();
	let v = VerifiedCommand2::fixture(a.generation, a.cluster, &cmd, &[], 0).unwrap();
	let ce = CommittedEntry2::fixture(at(1, 1, 2), fp, EntryClass2::Bootstrap(d));
	let s = bootstrap2(&pre_root(), &ce, &e, &v, &cmd).unwrap().0;
	(cmd, e, ce, s)
}
pub(super) fn chain_of(bytes: &[u8], length: u64) -> Chain {
	let bytes = bytes.to_vec();
	let any = AnyAdminManifest2::decode(&bytes).unwrap();
	let pin = exact_pin(&bytes);
	let (cmd, entry, committed, rooted) = rooted(&bytes, length);
	let observation = observe_bootstrap2(&rooted).unwrap();
	let record = BootstrapRecord2 {
		identity: observation.identity,
		inventory_seq: 2,
	};
	let a = any.admission();
	let snapshot = encode_snapshot2(&LogState2 {
		roots: vec![RootEntry2 {
			terminal: None,
			identity: RootIdentity2 {
				generation: a.generation,
				cluster: a.cluster,
				pin,
				admin_key: a.admin_key,
			},
			manifest: any.clone(),
			state: RootState2::Bound,
			last_seq: 2,
			record: Some(record),
		}],
		last_seq: 2,
	})
	.unwrap();
	let tuple = validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(&bytes),
		manifest_pin: Some(&pin),
		snapshot: Some(&snapshot),
		inventory_pin: Some(&hash(&snapshot)),
	})
	.unwrap();
	Chain {
		any,
		cmd,
		entry,
		committed,
		rooted,
		tuple,
	}
}
fn authority() -> AuthorityRef2 {
	AuthorityRef2 {
		id: [0x41; 16],
		revision: 1,
	}
}
/// FIXTURE: a rooted state with authority 0x41 rev 1 enrolled at (1,1,3).
pub(super) fn enrolled(
	rooted: &MutationAuthorityState2,
	any: &AnyAdminManifest2,
) -> MutationAuthorityState2 {
	let a = any.admission();
	let cmd = Command2::EnrollAuthority {
		authority: authority(),
		node: NodeRef2 {
			node: 7,
			incarnation: [0x17; 16],
		},
		key: [0x2B; 32],
	};
	let e = TrustedEntry2::fixture(a.generation, a.cluster, l(3), &cmd).unwrap();
	let v = VerifiedCommand2::fixture(a.generation, a.cluster, &cmd, &[], 0).unwrap();
	advance_root2(rooted, &e, &v, &cmd).unwrap().0
}
/// A Grant for authority 0x41 in the administrator's exact context and namespace, signed with
/// `seed` over the published preimage.
pub(super) fn grant(admin: &InstalledAdmin2, seed: u8) -> Command2 {
	let ctx = admin_context2(admin);
	let ns = admin_namespace2(admin);
	let key = PermissionKey2 {
		authority: authority(),
		namespace: AdmissionNamespace2 {
			generation: ctx.generation,
			tenant: ns.tenant,
			scope: ns.scope,
			table: ns.table,
			schema: ns.schema,
		},
	};
	let unsigned = encode_admin_statement2(AdminAction2::Grant, &key, &ctx, &[0; 64]).unwrap();
	let preimage = admin_statement_preimage2(&unsigned).unwrap();
	let signature = SigningKey::from_bytes(&[seed; 32])
		.sign(&preimage)
		.to_bytes();
	Command2::AdmissionPermission(
		encode_admin_statement2(AdminAction2::Grant, &key, &ctx, &signature).unwrap(),
	)
}
pub(super) fn reduce_at(
	state: &MutationAuthorityState2,
	admin: &InstalledAdmin2,
	cmd: &Command2,
	index: u64,
) -> Result2<ApplyResult2> {
	let ctx = admin_context2(admin);
	let e = TrustedEntry2::fixture(ctx.generation, ctx.cluster, l(index), cmd).unwrap();
	let v = VerifiedCommand2::fixture(ctx.generation, ctx.cluster, cmd, &[], 0).unwrap();
	Root2::reduce_admission(state, admin, &e, &v, cmd).map(|(_, r)| r)
}
/// The gate's expected context, rebuilt test-side from a state.
pub(super) fn expected_context(state: &MutationAuthorityState2) -> AdminContext2 {
	let (root, origin) = root_and_origin2(state).unwrap();
	let s = &root.state;
	AdminContext2 {
		generation: s.generation,
		cluster: s.cluster,
		installed_at: s.bootstrap,
		bootstrap_fingerprint: origin.bootstrap_fingerprint,
		bootstrap_command: origin.bootstrap_command,
		genesis: origin.genesis,
		initial_epoch: s.initial_epoch,
		manifest_pin: s.control_digest,
	}
}
pub(super) fn control_len(state: &MutationAuthorityState2) -> u64 {
	root_and_origin2(state).unwrap().0.state.control_len
}

impl Fixture {
	/// A fixture root bootstrapped with this exact profile-3 manifest.
	pub(in crate::storage::mutation_visibility::root2) fn profile_three(manifest: &[u8]) -> Self {
		assert!(matches!(
			AnyAdminManifest2::decode(manifest),
			Ok(AnyAdminManifest2::Three(_))
		));
		Self::from_manifest(manifest.to_vec())
	}
	pub(in crate::storage::mutation_visibility::root2) fn from_manifest(manifest: Vec<u8>) -> Self {
		let any = AnyAdminManifest2::decode(&manifest).unwrap();
		let a = any.admission();
		let dep = Deployment2 {
			generation: a.generation,
			cluster: a.cluster,
		};
		let boot = Command2::Bootstrap {
			epoch: a.initial_epoch,
			control: exact_pin(&manifest),
			length: manifest.len() as u64,
		};
		let entries = vec![entry(0), entry(1), carrier(&boot, dep, 2, None)];
		let source = CommittedSource2::synthetic();
		let fingerprint = |e: &E| {
			committed_entry2(&source, e)
				.unwrap()
				.committed()
				.entry_fields()
				.1
		};
		// Rebind the existing owner-approved record fixture to these exact immutable entries.
		let chain = chain_of(&manifest, manifest.len() as u64);
		let mut record = tuple_record2(&chain.tuple);
		record.identity.genesis_fingerprint = fingerprint(&entries[0]);
		record.identity.bootstrap_fingerprint = fingerprint(&entries[2]);
		record.identity.bootstrap_command = boot.digest(dep.generation, dep.cluster).unwrap();
		let pin = tuple_for(&manifest, record);
		let mut state = empty_authority2();
		for e in &entries[..2] {
			let ce = committed_entry2(&source, e).unwrap().into_committed();
			state = advance_pre_root2(&state, &ce).unwrap().0;
		}
		let ce = committed_entry2(&source, &entries[2])
			.unwrap()
			.into_committed();
		let trusted = TrustedEntry2::bind(&ce, &boot, dep.generation, dep.cluster).unwrap();
		state = advance_verified2(&state, &pin, &ce, &trusted, &boot, None)
			.unwrap()
			.0;
		Self {
			manifest,
			any,
			record,
			pin,
			dep,
			state,
			entries,
		}
	}
}
