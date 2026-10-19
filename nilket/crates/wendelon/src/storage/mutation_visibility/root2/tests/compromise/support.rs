//! A profile-3 root built from real carrier entries through the sole production verifier, then
//! written as real Control WAL records into a fresh data directory, and audited through the
//! `wendelon audit root2` executor. Signing here is test-only; each proof states its claimed
//! role and epoch and the seed that signs it, so a stale or forged signer is explicit.
use super::wire;
use crate::consensus::raft_storage::read_only_fixture;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AdminManifestThree3, GovernanceRole2, RecoveryRole3,
};
use crate::storage::mutation_visibility::root2::admin_install::{
	InstalledAdmin2, admin_context2, admin_namespace2, install_admin2,
};
use crate::storage::mutation_visibility::root2::admin_statement::{
	AdminAction2, AdmissionNamespace2, PermissionKey2, admin_statement_preimage2,
	encode_admin_statement2, epoch_statement_preimage3,
};
use crate::storage::mutation_visibility::root2::command::{ApplyResult2, Command2};
use crate::storage::mutation_visibility::root2::origin::root_and_origin2;
use crate::storage::mutation_visibility::root2::role_command::{
	RoleForm3, context_digest, preimage_for,
};
use crate::storage::mutation_visibility::root2::types::{Digest2, Log2};
use crate::test_support::TestDir;
use ed25519_dalek::{Signer, SigningKey};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub(super) const ADMIN: u8 = 1;
pub(super) const GOVERNANCE: u8 = 2;
pub(super) const RECOVERY: u8 = 3;
pub(super) fn key(seed: u8) -> Digest2 {
	SigningKey::from_bytes(&[seed; 32])
		.verifying_key()
		.to_bytes()
}
fn zero() -> Log2 {
	Log2 {
		term: 0,
		node: 0,
		index: 0,
	}
}
/// One proof slot: claimed role (0 empty), claimed epoch, signing seed.
pub(super) type Slot = (u8, u64, u8);
pub(super) struct Root {
	pub(super) f: wire::Fixture,
	/// The installed administrator view, fixed at the install entry (the root's context).
	admin: InstalledAdmin2,
	/// Current signing seed and epoch per role (admin, governance, recovery).
	pub(super) seeds: [u8; 3],
	pub(super) epochs: [u64; 3],
	issuers: u32,
}
impl Root {
	pub(super) fn new() -> Self {
		let old = wire::Fixture::new("b");
		let mut base = old.any.admission().clone();
		base.admin_key = key(11);
		let manifest = AdminManifestThree3::new(
			base,
			GovernanceRole2::new([0xd2; 16], key(12)).unwrap(),
			RecoveryRole3::new([0xd3; 16], key(13)).unwrap(),
		)
		.unwrap()
		.encode()
		.unwrap()
		.to_vec();
		let f = wire::Fixture::from_manifest(manifest);
		let admin = install_admin2(&f.state, &f.pin).unwrap();
		Self {
			f,
			admin,
			seeds: [11, 12, 13],
			epochs: [0; 3],
			issuers: 0,
		}
	}
	/// The index the next applied entry takes.
	pub(super) fn next(&self) -> u64 {
		self.f.entries.len() as u64
	}
	pub(super) fn current(&self, role: u8) -> Slot {
		let i = usize::from(role - 1);
		(role, self.epochs[i], self.seeds[i])
	}
	fn digest(&self, c: &Command2) -> Digest2 {
		c.digest(self.f.dep.generation, self.f.dep.cluster).unwrap()
	}
	/// A `VISROL03` proof for `c` with exactly these claims and signers.
	pub(super) fn proof(&self, c: &Command2, slots: [Slot; 2]) -> Vec<u8> {
		let form = match c {
			Command2::RetireRoot => RoleForm3::Retire,
			Command2::Freeze(_) => RoleForm3::Freeze,
			Command2::Unfreeze(_) => RoleForm3::Unfreeze,
			_ => RoleForm3::Legacy,
		};
		let mut b = vec![0; 224];
		b[..8].copy_from_slice(b"VISROL03");
		b[8..10].copy_from_slice(&1u16.to_le_bytes());
		b[12..44].copy_from_slice(&context_digest(&self.admin));
		b[44..76].copy_from_slice(&self.digest(c));
		for ((role, epoch, _), at) in slots.into_iter().zip([76, 150]) {
			b[at] = role;
			if role != 0 {
				b[at + 1..at + 9].copy_from_slice(&epoch.to_le_bytes());
				b[at + 9] = 1;
			}
		}
		let roles = [slots[0].0, slots[1].0];
		for (i, ((role, _, seed), at)) in slots.into_iter().zip([76, 150]).enumerate() {
			if role != 0
				&& let Ok(pre) = preimage_for(&b, roles, i, form)
			{
				let sig = SigningKey::from_bytes(&[seed; 32]).sign(&pre).to_bytes();
				b[at + 10..at + 74].copy_from_slice(&sig);
			}
		}
		b
	}
	pub(super) fn apply(&mut self, c: Command2, sidecar: Option<Vec<u8>>) -> ApplyResult2 {
		self.f.apply(c, sidecar)
	}
	fn transition(&self) -> Log2 {
		root_and_origin2(&self.f.state)
			.unwrap()
			.0
			.state
			.role_epochs
			.as_ref()
			.map_or(zero(), |e| e.transition)
	}
	/// A governance command (an issuer enrolment) signed by these `VISROL03` claims.
	pub(super) fn governance_use(&mut self, signer: Slot) -> ApplyResult2 {
		self.issuers += 1;
		let c = Command2::EnrollLeaseIssuer {
			node: 100 + self.issuers,
			key: key(0x60 + self.issuers as u8),
		};
		let p = self.proof(&c, [signer, (0, 0, 0)]);
		self.apply(c, Some(p))
	}
	/// Appends a carrier entry without verifying it, for histories the production verifier refuses.
	pub(super) fn push_unverified(&mut self, c: &Command2, sidecar: Option<&[u8]>) {
		let e = wire::carrier(c, self.f.dep, self.next(), sidecar);
		self.f.entries.push(e);
	}
	/// Governance enrols the Admission authority administrator statements name.
	pub(super) fn enrol_authority(&mut self) {
		let c = wire::enroll_authority();
		let p = self.proof(&c, [self.current(GOVERNANCE), (0, 0, 0)]);
		applied(self.apply(c, Some(p)));
	}
	/// A governance command under the legacy `VISADG01` sidecar, signed by `seed`.
	pub(super) fn legacy_governance_use(&mut self, seed: u8) -> ApplyResult2 {
		self.issuers += 1;
		let c = Command2::EnrollLeaseIssuer {
			node: 100 + self.issuers,
			key: key(0x60 + self.issuers as u8),
		};
		let sidecar = wire::sign_context(admin_context2(&self.admin), self.digest(&c), seed);
		self.apply(c, Some(sidecar))
	}
	fn permission_key(&self) -> PermissionKey2 {
		let c = admin_context2(&self.admin);
		let ns = admin_namespace2(&self.admin);
		PermissionKey2 {
			authority: wire::authority(),
			namespace: AdmissionNamespace2 {
				generation: c.generation,
				tenant: ns.tenant,
				scope: ns.scope,
				table: ns.table,
				schema: ns.schema,
			},
		}
	}
	/// A legacy `VISADS02` administrator statement signed by `seed` (claims admin epoch 0).
	pub(super) fn statement_use(&mut self, seed: u8) -> ApplyResult2 {
		let c = admin_context2(&self.admin);
		let k = self.permission_key();
		let unsigned = encode_admin_statement2(AdminAction2::Grant, &k, &c, &[0; 64]).unwrap();
		let sig = SigningKey::from_bytes(&[seed; 32])
			.sign(&admin_statement_preimage2(&unsigned).unwrap())
			.to_bytes();
		let s = encode_admin_statement2(AdminAction2::Grant, &k, &c, &sig).unwrap();
		self.apply(Command2::AdmissionPermission(s), None)
	}
	/// A `VISADS03` statement claiming admin `epoch`, signed by `seed`.
	pub(super) fn epoch_statement_use(&mut self, seed: u8, epoch: u64) -> ApplyResult2 {
		let c = admin_context2(&self.admin);
		let old =
			encode_admin_statement2(AdminAction2::Grant, &self.permission_key(), &c, &[0; 64])
				.unwrap();
		let mut b = [0; 345];
		b[..273].copy_from_slice(&old[..273]);
		b[..8].copy_from_slice(b"VISADS03");
		b[273..281].copy_from_slice(&epoch.to_le_bytes());
		let sig = SigningKey::from_bytes(&[seed; 32])
			.sign(&epoch_statement_preimage3(&b).unwrap())
			.to_bytes();
		b[281..].copy_from_slice(&sig);
		self.apply(Command2::EpochAdmissionPermission(b), None)
	}
	/// Rotates `role` to the key of `seed`: admin and governance by recovery, recovery jointly.
	pub(super) fn rotate(&mut self, role: u8, seed: u8) {
		let c = match role {
			ADMIN => Command2::RotateAdmin(key(seed)),
			GOVERNANCE => Command2::RotateGovernance(key(seed)),
			_ => Command2::RotateRecovery(key(seed)),
		};
		let slots = if role == RECOVERY {
			[self.current(ADMIN), self.current(GOVERNANCE)]
		} else {
			[self.current(RECOVERY), (0, 0, 0)]
		};
		let p = self.proof(&c, slots);
		let at = self.next();
		assert!(self.apply(c, Some(p)) == ApplyResult2::Registered(wire::log(at)));
		let i = usize::from(role - 1);
		self.seeds[i] = seed;
		self.epochs[i] += 1;
	}
	pub(super) fn freeze(&mut self) {
		let c = Command2::Freeze(self.transition());
		let p = self.proof(&c, [self.current(RECOVERY), (0, 0, 0)]);
		let at = self.next();
		assert!(self.apply(c, Some(p)) == ApplyResult2::Registered(wire::log(at)));
	}
	pub(super) fn unfreeze(&mut self) {
		let c = Command2::Unfreeze(self.transition());
		let p = self.proof(&c, [self.current(ADMIN), self.current(RECOVERY)]);
		let at = self.next();
		assert!(self.apply(c, Some(p)) == ApplyResult2::Registered(wire::log(at)));
	}
	/// An Unfreeze while not frozen: both signatures valid, a consumed `Denied(24)`.
	pub(super) fn idle_unfreeze(&mut self) -> ApplyResult2 {
		let c = Command2::Unfreeze(self.transition());
		let p = self.proof(&c, [self.current(ADMIN), self.current(RECOVERY)]);
		self.apply(c, Some(p))
	}
	pub(super) fn retire(&mut self) {
		let c = Command2::RetireRoot;
		let p = self.proof(&c, [self.current(RECOVERY), (0, 0, 0)]);
		let at = self.next();
		assert!(self.apply(c, Some(p)) == ApplyResult2::Registered(wire::log(at)));
	}
	pub(super) fn manifest(&self) -> &[u8] {
		&self.f.manifest
	}
	/// The installed administrator view: the root's context.
	pub(super) fn admin(&self) -> &InstalledAdmin2 {
		&self.admin
	}
	/// Writes the whole history as one real WAL segment committing through `commit` (default:
	/// the last entry), plus the trusted manifest outside the data directory.
	pub(super) fn write(&self, commit: Option<u64>) -> Dirs {
		let dirs = Dirs::new(self.manifest());
		let commit = commit.unwrap_or(self.next() - 1);
		read_only_fixture::write_segment(
			&dirs.data,
			0,
			&self.f.entries,
			Some(self.f.entries[commit as usize].log_id),
		);
		dirs
	}
}
pub(super) fn deny(r: ApplyResult2, reason: u16) {
	assert!(
		r == ApplyResult2::Denied {
			reason,
			reference: None
		},
		"expected Denied({reason})"
	);
}
pub(super) fn applied(r: ApplyResult2) {
	assert!(
		!matches!(r, ApplyResult2::Denied { .. }),
		"expected an applied result"
	);
}

/// A fresh scratch root with a data directory and a trust-input directory beside it.
pub(super) struct Dirs {
	_base: TestDir,
	pub(super) data: PathBuf,
	pub(super) inputs: PathBuf,
	pub(super) manifest: PathBuf,
}
impl Dirs {
	pub(super) fn new(manifest: &[u8]) -> Self {
		let base = TestDir::new("root2-compromise-audit");
		let data = base.join("data");
		let inputs = base.join("inputs");
		std::fs::create_dir(&data).unwrap();
		std::fs::create_dir(&inputs).unwrap();
		let path = inputs.join("manifest.bin");
		std::fs::write(&path, manifest).unwrap();
		Self {
			_base: base,
			data,
			inputs,
			manifest: path,
		}
	}
	pub(super) fn key_file(&self, key: &Digest2) -> PathBuf {
		let path = self.inputs.join("suspect.key");
		let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
		std::fs::write(&path, format!("{hex}\n")).unwrap();
		path
	}
}

/// Every file under `dir`: its bytes and modification time.
pub(super) fn listing(dir: &Path) -> BTreeMap<PathBuf, (Vec<u8>, SystemTime)> {
	let mut out = BTreeMap::new();
	let mut stack = vec![dir.to_path_buf()];
	while let Some(d) = stack.pop() {
		for e in std::fs::read_dir(&d).unwrap() {
			let p = e.unwrap().path();
			let meta = std::fs::symlink_metadata(&p).unwrap();
			if meta.is_dir() {
				stack.push(p.clone());
			}
			let bytes = if meta.is_file() {
				std::fs::read(&p).unwrap()
			} else {
				Vec::new()
			};
			out.insert(p, (bytes, meta.modified().unwrap()));
		}
	}
	out
}

/// One CLI run: every stdout line parsed, the final coverage record split out.
pub(super) struct Out {
	pub(super) stdout: String,
	pub(super) stderr: String,
	pub(super) code: u8,
	pub(super) rows: Vec<Value>,
	pub(super) coverage: Option<Value>,
}
impl Out {
	pub(super) fn row(&self, index: u64) -> &Value {
		self.rows
			.iter()
			.find(|r| r["log_id"]["index"] == index)
			.unwrap_or_else(|| panic!("no row at {index}"))
	}
	pub(super) fn class(&self, index: u64) -> &str {
		self.row(index)["match"].as_str().unwrap()
	}
	/// Indices of the rows with this match class, in output order.
	pub(super) fn indices(&self, class: &str) -> Vec<u64> {
		self.rows
			.iter()
			.filter(|r| r["match"] == class)
			.map(|r| r["log_id"]["index"].as_u64().unwrap())
			.collect()
	}
	pub(super) fn containment(&self, role: &str, epoch: u64) -> &Value {
		self.coverage.as_ref().unwrap()["containment"]
			.as_array()
			.unwrap()
			.iter()
			.find(|c| c["identity"]["role"] == role && c["identity"]["epoch"] == epoch)
			.unwrap()
	}
}
fn role_name(role: u8) -> &'static str {
	match role {
		ADMIN => "admin",
		GOVERNANCE => "governance",
		_ => "recovery",
	}
}
/// `--role R --epoch E` arguments for these identities.
pub(super) fn suspects(ids: &[(u8, u64)]) -> Vec<String> {
	ids.iter()
		.flat_map(|(r, e)| {
			[
				"--role".to_string(),
				role_name(*r).to_string(),
				"--epoch".to_string(),
				e.to_string(),
			]
		})
		.collect()
}
/// `--export-context <out> --node-id <node>`.
pub(super) fn export_args(out: &Path, node: u32) -> Vec<String> {
	vec![
		"--export-context".into(),
		out.display().to_string(),
		"--node-id".into(),
		node.to_string(),
	]
}
/// Runs `wendelon audit root2 <data> --trusted-manifest <file> <extra...>` in process.
pub(super) fn audit(dirs: &Dirs, extra: &[String]) -> Out {
	audit_with(&dirs.data, &dirs.manifest, extra)
}
pub(super) fn audit_with(
	data: &std::path::Path,
	manifest: &std::path::Path,
	extra: &[String],
) -> Out {
	let mut args = vec![
		data.display().to_string(),
		"--trusted-manifest".to_string(),
		manifest.display().to_string(),
	];
	args.extend_from_slice(extra);
	let (mut out, mut err) = (Vec::new(), Vec::new());
	let code = crate::cli::root2_audit::execute(&args, &mut out, &mut err);
	let stdout = String::from_utf8(out).unwrap();
	let mut rows: Vec<Value> = stdout
		.lines()
		.map(|l| serde_json::from_str(l).unwrap())
		.collect();
	let coverage = rows.last().filter(|v| v["kind"] == "coverage").cloned();
	if coverage.is_some() {
		rows.pop();
	}
	assert!(rows.iter().all(|r| r["kind"] == "entry"));
	Out {
		stdout,
		stderr: String::from_utf8(err).unwrap(),
		code,
		rows,
		coverage,
	}
}
/// The audit invariants every outcome must keep: no `complete` record outside exit 0, and
/// stderr exactly when the exit code is 1, 2 or 4.
pub(super) fn check_outcome(out: &Out) {
	let status = out.coverage.as_ref().map(|c| c["status"].clone());
	match out.code {
		0 => assert_eq!(status, Some(Value::from("complete"))),
		3 => assert_eq!(status, Some(Value::from("partial"))),
		_ => {
			assert!(status != Some(Value::from("complete")));
			assert!(!out.stderr.is_empty());
		}
	}
	if out.code == 0 || out.code == 3 {
		assert!(out.stderr.is_empty(), "{}", out.stderr);
	}
}
