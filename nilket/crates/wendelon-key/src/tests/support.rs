//! Fixtures: a scripted `Io`, a custody tree per test, and context exports of a real profile-3
//! root (the published `M3_S1` manifest, D-G3 test seeds) whose successors are signed through the
//! facade's own `prepare` and `splice`.
use crate::commands;
use crate::error::ToolError;
use crate::host::HostFacts;
use crate::io::Io;
use ed25519_dalek::{Signer, SigningKey};
use key_guard::custody::{CustodyDir, SecretBuf};
use std::collections::VecDeque;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use wendelon::storage::mutation_visibility::root2::Command2;
use wendelon::storage::mutation_visibility::root2::operator_proof::{
	self as facade, verify_context_export,
};

pub(crate) const ADMIN_SEED: [u8; 32] = [0xe1; 32];
pub(crate) const GOVERNANCE_SEED: [u8; 32] = [0xe2; 32];
pub(crate) const RECOVERY_SEED: [u8; 32] = [0xe3; 32];
pub(crate) const PRIMARY: &str = "correct horse battery staple one";
pub(crate) const SECONDARY: &str = "a different backup passphrase two";
pub(crate) const NEWER: &str = "the restored primary passphrase three";

/// `M3_S1` from wendelon's `root2/tests/profile_three_fixture.rs`: generation 0x11, cluster 0x12,
/// initial epoch 1, keyed by the D-G3 seeds (administrator 0xe1, governance 0xe2, recovery 0xe3).
const M3_S1: &str = concat!(
	"56495341444d3200010003001111111111111111111111111111111112121212121212121212121212121212",
	"01000000000000005151515151515151515151515151515125fc32c478a5a4f84e4557a0342e08278c883641",
	"b6149bdb0b4a986ab6c2cb780300000001000000000000000000000000000000000000000000000000000000",
	"000000000052525252525252525252525252525252c91cb3ce2b84e4ba85f562ece41edfe4e27afc52d88d50",
	"7f66a18638df823e9fff010000535353535353535353535353535353534dd9b6496a27571acb089a5e3482df",
	"c86acdeddc4d1f28e15a9b04f026d7f2261f000000",
);
pub(crate) const GENERATION: [u8; 16] = [0x11; 16];
pub(crate) const CLUSTER: [u8; 16] = [0x12; 16];

pub(crate) fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
pub(crate) fn hex(b: &[u8]) -> String {
	crate::envelope::hex(b)
}
pub(crate) fn public(seed: &[u8; 32]) -> [u8; 32] {
	SigningKey::from_bytes(seed).verifying_key().to_bytes()
}
pub(crate) fn manifest() -> Vec<u8> {
	unhex(M3_S1)
}
fn sha256(parts: &[&[u8]]) -> [u8; 32] {
	let mut ctx = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
	for p in parts {
		ctx.update(p);
	}
	ctx.finish().as_ref().try_into().unwrap()
}

/// The 188-byte context of the fixture root, with `genesis` varied to make another root.
pub(crate) fn context_bytes(genesis: u8) -> [u8; 188] {
	let mut b = Vec::new();
	b.extend(GENERATION);
	b.extend(CLUSTER);
	b.extend(1u64.to_le_bytes());
	b.extend(1u32.to_le_bytes());
	b.extend(5u64.to_le_bytes());
	b.extend([0xa1; 32]);
	b.extend([0xa2; 32]);
	b.extend([genesis; 32]);
	b.extend(1u64.to_le_bytes());
	b.extend(sha256(&[b"VISROOT2-ADMIN-MANIFEST\0", &manifest()]));
	b.try_into().unwrap()
}

#[derive(Clone)]
pub(crate) struct Successor {
	pub(crate) role: u8,
	pub(crate) epoch: u64,
	pub(crate) key: [u8; 32],
	pub(crate) index: u64,
	pub(crate) command: Vec<u8>,
	pub(crate) sidecar: Vec<u8>,
}

/// A profile-3 root as context exports describe it: the context, the active seed per role, and
/// the re-verifiable successors so far.
#[derive(Clone)]
pub(crate) struct World {
	pub(crate) context: [u8; 188],
	pub(crate) seeds: [[u8; 32]; 3],
	pub(crate) successors: Vec<Successor>,
	pub(crate) coverage: u8,
	pub(crate) frozen: bool,
	pub(crate) retired: bool,
}

fn log20(b: &mut Vec<u8>, term: u64, node: u32, index: u64) {
	b.extend(term.to_le_bytes());
	b.extend(node.to_le_bytes());
	b.extend(index.to_le_bytes());
}

impl World {
	pub(crate) fn genesis() -> Self {
		Self::of(0xa3)
	}
	/// Another root over the same manifest: a different genesis digest in its context.
	pub(crate) fn other_root() -> Self {
		Self::of(0xa4)
	}
	fn of(genesis: u8) -> Self {
		Self {
			context: context_bytes(genesis),
			seeds: [ADMIN_SEED, GOVERNANCE_SEED, RECOVERY_SEED],
			successors: Vec::new(),
			coverage: 1,
			frozen: false,
			retired: false,
		}
	}
	/// The C-S6b version-1 export as node `node_id` writes it, sealed with its trailing digest.
	pub(crate) fn export(&self, node_id: u32) -> Vec<u8> {
		let mut b = b"WRCTXEXP".to_vec();
		b.extend(1u16.to_le_bytes());
		b.extend([0, 0]);
		b.extend(self.context);
		b.extend(manifest());
		b.extend(node_id.to_le_bytes());
		log20(&mut b, 1, node_id, 100);
		b.push(self.coverage);
		b.push(u8::from(self.frozen));
		b.push(0);
		log20(&mut b, 0, 0, 0);
		b.push(u8::from(self.retired));
		if self.retired {
			log20(&mut b, 1, 1, 99);
		} else {
			log20(&mut b, 0, 0, 0);
		}
		b.extend((self.successors.len() as u32).to_le_bytes());
		for s in &self.successors {
			b.push(s.role);
			b.extend(s.epoch.to_le_bytes());
			b.extend(s.key);
			log20(&mut b, 1, 1, s.index);
			b.extend((s.command.len() as u32).to_le_bytes());
			b.extend(&s.command);
			b.extend((s.sidecar.len() as u32).to_le_bytes());
			b.extend(&s.sidecar);
		}
		let digest = sha256(&[&b]);
		b.extend(digest);
		b
	}
	/// Rotates `role` (admin or governance) to `seed`'s key under the recovery key, signed through
	/// the facade over the export so far.
	pub(crate) fn rotate(&mut self, role: u8, seed: [u8; 32]) {
		let key = public(&seed);
		let command = match role {
			1 => Command2::RotateAdmin(key),
			2 => Command2::RotateGovernance(key),
			_ => panic!("the fixture rotates admin or governance only"),
		};
		let command = command.encode(GENERATION, CLUSTER).unwrap();
		let ctx = verify_context_export(&self.export(1), &manifest()).unwrap();
		let recovery = self.seeds[2];
		let unsigned =
			facade::prepare(&ctx, &command, &public(&recovery), None, None, None).unwrap();
		let signature = SigningKey::from_bytes(&recovery)
			.sign(unsigned.preimage())
			.to_bytes();
		let sidecar = facade::splice(&unsigned, &signature).unwrap();
		facade::self_check(&ctx, &command, Some(&sidecar)).unwrap();
		let epoch = self.successors.iter().filter(|s| s.role == role).count() as u64 + 1;
		self.successors.push(Successor {
			role,
			epoch,
			key,
			index: 10 + self.successors.len() as u64,
			command,
			sidecar,
		});
		self.seeds[usize::from(role - 1)] = seed;
	}
	/// Rotates the recovery key to `seed`'s key under the joint administrator and governance
	/// proof: each host signs its own slot through the facade, the second over the first's partial.
	pub(crate) fn rotate_recovery(&mut self, seed: [u8; 32]) {
		let key = public(&seed);
		let command = Command2::RotateRecovery(key)
			.encode(GENERATION, CLUSTER)
			.unwrap();
		let ctx = verify_context_export(&self.export(1), &manifest()).unwrap();
		let sign = |signer: [u8; 32], partial: Option<&[u8]>| {
			let u = facade::prepare(&ctx, &command, &public(&signer), None, None, partial).unwrap();
			let s = SigningKey::from_bytes(&signer)
				.sign(u.preimage())
				.to_bytes();
			facade::splice(&u, &s).unwrap()
		};
		let first = sign(self.seeds[0], None);
		let sidecar = sign(self.seeds[1], Some(&first));
		facade::self_check(&ctx, &command, Some(&sidecar)).unwrap();
		let epoch = self.successors.iter().filter(|s| s.role == 3).count() as u64 + 1;
		self.successors.push(Successor {
			role: 3,
			epoch,
			key,
			index: 10 + self.successors.len() as u64,
			command,
			sidecar,
		});
		self.seeds[2] = seed;
	}
}

static NEXT: AtomicU64 = AtomicU64::new(0);

/// A fresh tree in `TMPDIR`: `root` (the walk's anchor) holding the custody directory `keys`, a
/// second custody directory `backups`, and plain files for exports and the manifest. Removed on
/// drop.
pub(crate) struct Fixture {
	pub(crate) root: PathBuf,
	pub(crate) keys: PathBuf,
	pub(crate) backups: PathBuf,
	pub(crate) manifest: PathBuf,
}

fn private_dir(path: &Path) {
	std::fs::DirBuilder::new().mode(0o700).create(path).unwrap();
	std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}

impl Fixture {
	pub(crate) fn new() -> Self {
		let root = std::env::temp_dir().join(format!(
			"wendelon-key-{}-{}",
			std::process::id(),
			NEXT.fetch_add(1, Ordering::Relaxed)
		));
		private_dir(&root);
		let keys = root.join("keys");
		let backups = root.join("backups");
		private_dir(&keys);
		private_dir(&backups);
		let manifest = root.join("manifest.bin");
		std::fs::write(&manifest, manifest_bytes()).unwrap();
		Self {
			root,
			keys,
			backups,
			manifest,
		}
	}
	/// Writes `bytes` as the plain input file `name` and returns its path as a string.
	pub(crate) fn file(&self, name: &str, bytes: &[u8]) -> String {
		let path = self.root.join(name);
		std::fs::write(&path, bytes).unwrap();
		path.display().to_string()
	}
	pub(crate) fn io(&self) -> TestIo {
		TestIo::new(&self.root)
	}
	pub(crate) fn dir(&self) -> String {
		self.keys.display().to_string()
	}
	pub(crate) fn manifest(&self) -> String {
		self.manifest.display().to_string()
	}
	pub(crate) fn exists(&self, rel: &str) -> bool {
		self.keys.join(rel).exists()
	}
	pub(crate) fn log(&self) -> Vec<u8> {
		std::fs::read(self.keys.join("custody.log")).unwrap_or_default()
	}
}

fn manifest_bytes() -> Vec<u8> {
	manifest()
}

impl Drop for Fixture {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.root);
	}
}

/// The scripted `Io`: secrets (passphrases and confirmations) and seeds in order, counter-made
/// salt and nonce bytes, a fixed host and clock, captured stdout and stderr.
pub(crate) struct TestIo {
	pub(crate) secrets: VecDeque<String>,
	pub(crate) seeds: VecDeque<[u8; 32]>,
	pub(crate) counter: u8,
	pub(crate) host: HostFacts,
	pub(crate) out: Vec<u8>,
	pub(crate) err: Vec<u8>,
	pub(crate) prompts: Vec<String>,
	/// When set, the host probe fails.
	pub(crate) host_fails: bool,
	anchor: PathBuf,
}

impl TestIo {
	pub(crate) fn new(anchor: &Path) -> Self {
		Self {
			secrets: VecDeque::new(),
			seeds: VecDeque::new(),
			counter: 0,
			host: HostFacts {
				hostname: "custody-test".into(),
				..HostFacts::default()
			},
			out: Vec::new(),
			err: Vec::new(),
			prompts: Vec::new(),
			host_fails: false,
			anchor: anchor.to_path_buf(),
		}
	}
	pub(crate) fn say(&mut self, lines: &[&str]) -> &mut Self {
		self.secrets.extend(lines.iter().map(|s| s.to_string()));
		self
	}
	pub(crate) fn seed(&mut self, seed: [u8; 32]) -> &mut Self {
		self.seeds.push_back(seed);
		self
	}
	/// Runs one command line; returns whether it succeeded, with stdout and stderr since the
	/// last call.
	pub(crate) fn run(&mut self, argv: &[&str]) -> (bool, String, String) {
		self.out.clear();
		self.err.clear();
		let argv: Vec<String> = argv.iter().map(|s| s.to_string()).collect();
		let code = commands::run(&argv, self);
		(
			code == std::process::ExitCode::SUCCESS,
			String::from_utf8_lossy(&self.out).into_owned(),
			String::from_utf8_lossy(&self.err).into_owned(),
		)
	}
	/// Runs `argv` and panics with its stderr unless it succeeds; returns stdout.
	pub(crate) fn ok(&mut self, argv: &[&str]) -> String {
		let (ok, out, err) = self.run(argv);
		assert!(ok, "{argv:?} failed: {err}");
		assert!(
			self.secrets.is_empty(),
			"{argv:?} left secrets: {:?}",
			self.secrets.len()
		);
		out
	}
	/// Runs `argv`, which must be refused with `needle` in its stderr; returns stderr.
	pub(crate) fn refused(&mut self, argv: &[&str], needle: &str) -> String {
		let (ok, _, err) = self.run(argv);
		assert!(!ok, "{argv:?} succeeded");
		assert!(err.contains(needle), "{argv:?}: {needle:?} not in {err}");
		self.secrets.clear();
		self.seeds.clear();
		err
	}
}

impl Io for TestIo {
	fn secret(&mut self, prompt: &str, max: usize) -> Result<SecretBuf, ToolError> {
		self.prompts.push(prompt.to_string());
		let line = self.secrets.pop_front().ok_or_else(|| {
			ToolError::Usage(format!("the test script has no entry for {prompt:?}"))
		})?;
		if line.len() > max {
			return Err(ToolError::PassphraseTooLong);
		}
		let mut buf = SecretBuf::with_capacity(max);
		for b in line.bytes() {
			buf.push(b).map_err(|_| ToolError::PassphraseTooLong)?;
		}
		Ok(buf)
	}
	fn terminal_ready(&mut self) -> Result<(), ToolError> {
		Ok(())
	}
	fn new_seed(&mut self) -> Result<SecretBuf, ToolError> {
		let seed = self
			.seeds
			.pop_front()
			.expect("the test script supplies every seed");
		let mut buf = SecretBuf::zeroed(32);
		buf.as_mut_slice().copy_from_slice(&seed);
		Ok(buf)
	}
	fn fill(&mut self, out: &mut [u8]) -> Result<(), ToolError> {
		for b in out {
			*b = self.counter;
			self.counter = self.counter.wrapping_add(1);
		}
		Ok(())
	}
	fn host(&mut self) -> Result<HostFacts, ToolError> {
		if self.host_fails {
			return Err(ToolError::read("the network interface list")(
				std::io::Error::other("probe failed"),
			));
		}
		Ok(self.host.clone())
	}
	fn now(&self) -> u64 {
		1_790_000_000
	}
	fn open_dir(&self, path: &Path) -> Result<CustodyDir, ToolError> {
		Ok(key_guard::custody::open_custody_dir_anchored(
			path,
			&self.anchor,
		)?)
	}
	fn out(&mut self) -> &mut dyn Write {
		&mut self.out
	}
	fn err(&mut self) -> &mut dyn Write {
		&mut self.err
	}
}
