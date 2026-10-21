//! The signing fixture: a root with Bound keys for all three roles in one custody directory, the
//! command and code helpers, and the nine governance kinds. Tests are in `signing_cells` (T-K7
//! and `build`), `signing_wys` (T-WYS, T-MERGE, the 5c leg of T-K5) and `signing_refusals`.
use super::support::*;
use crate::envelope::{Header, NEW_LOG_N, NEW_P, State};
use std::sync::atomic::{AtomicU64, Ordering};
use wendelon::storage::mutation_visibility::root2::operator_proof::{
	self as facade, VerifiedContext, verify_context_export,
};
use wendelon::storage::mutation_visibility::root2::{
	AuthorityRef2, Command2, GrantRef2, Hlc2, NodeRef2, SignedLease2,
};
use wendelon::storage::mutation_visibility::{
	DecisionLogId, MutationIdentity, MutationScope, ProofKeyBinding,
};

static FILES: AtomicU64 = AtomicU64::new(0);

/// A root with Bound keys in its custody directory: `admin0`, `gov0`, `rec0` (the D-G3 seeds).
pub(crate) struct Root {
	pub(crate) fx: Fixture,
	pub(crate) io: TestIo,
	pub(crate) world: World,
}

pub(crate) fn log(index: u64) -> DecisionLogId {
	DecisionLogId {
		term: 1,
		node: 7,
		index,
	}
}

impl Root {
	pub(crate) fn new() -> Self {
		let fx = Fixture::new();
		let io = fx.io();
		let world = World::genesis();
		let a = fx.file("genesis-a", &world.export(1));
		let b = fx.file("genesis-b", &world.export(2));
		let mut root = Self { fx, io, world };
		for (name, role, seed) in [
			("admin", "admin", ADMIN_SEED),
			("gov", "governance", GOVERNANCE_SEED),
			("rec", "recovery", RECOVERY_SEED),
		] {
			root.generate(&format!("{name}.p"), role, "0", seed);
			root.bind(&format!("{name}.p"), &format!("{name}0"), &[&a, &b]);
		}
		root
	}
	pub(crate) fn d(&self) -> String {
		self.fx.dir()
	}
	pub(crate) fn file(&self, bytes: &[u8]) -> String {
		self.fx.file(
			&format!("in-{}", FILES.fetch_add(1, Ordering::Relaxed)),
			bytes,
		)
	}
	/// The current export, as a file.
	pub(crate) fn ctx(&self) -> String {
		self.file(&self.world.export(1))
	}
	pub(crate) fn verified(&self) -> VerifiedContext {
		verify_context_export(&self.world.export(1), &manifest()).unwrap()
	}
	pub(crate) fn generate(&mut self, out: &str, role: &str, epoch: &str, seed: [u8; 32]) {
		self.io.say(&[PRIMARY, PRIMARY]).seed(seed);
		let d = self.d();
		self.io.ok(&[
			"generate",
			"--dir",
			&d,
			"--role",
			role,
			"--intended-epoch",
			epoch,
			"--out",
			out,
		]);
	}
	pub(crate) fn bind(&mut self, key: &str, out: &str, contexts: &[&str]) {
		self.io.say(&[PRIMARY]);
		let (d, m) = (self.d(), self.fx.manifest());
		let mut argv = vec![
			"bind",
			"--dir",
			&d,
			"--key",
			key,
			"--out",
			out,
			"--trusted-manifest",
			&m,
		];
		for c in contexts {
			argv.extend(["--context", c]);
		}
		self.io.ok(&argv);
	}
	/// Rotates `role` to `seed` in the world and binds its Pending file as `out`.
	pub(crate) fn rotate_and_bind(&mut self, role: u8, seed: [u8; 32], out: &str) {
		let name = ["admin", "governance", "recovery"][usize::from(role - 1)];
		let epoch = (self.verified().active()[usize::from(role - 1)].0 + 1).to_string();
		self.generate(&format!("{out}.p"), name, &epoch, seed);
		match role {
			3 => self.world.rotate_recovery(seed),
			r => self.world.rotate(r, seed),
		}
		let ctx = self.ctx();
		self.bind(&format!("{out}.p"), out, &[&ctx]);
	}
	pub(crate) fn command(&self, c: &Command2) -> (Vec<u8>, String) {
		let bytes = c.encode(GENERATION, CLUSTER).unwrap();
		let path = self.file(&bytes);
		(bytes, path)
	}
	/// Runs `sign` with `key` over `command`, typing `code`, writing `out`.
	pub(crate) fn sign(
		&mut self,
		key: &str,
		command: &str,
		code: &str,
		out: &str,
		extra: &[&str],
	) -> (bool, String, String) {
		self.sign_in(&self.ctx(), key, command, code, out, extra)
	}
	pub(crate) fn sign_in(
		&mut self,
		ctx: &str,
		key: &str,
		command: &str,
		code: &str,
		out: &str,
		extra: &[&str],
	) -> (bool, String, String) {
		self.io.say(&[PRIMARY, code]);
		let (d, m) = (self.d(), self.fx.manifest());
		let mut argv = vec![
			"sign",
			"--dir",
			&d,
			"--key",
			key,
			"--context",
			ctx,
			"--trusted-manifest",
			&m,
			"--command",
			command,
			"--out",
			out,
		];
		argv.extend(extra);
		let r = self.io.run(&argv);
		self.io.secrets.clear();
		r
	}
	pub(crate) fn statement(&mut self, key: &str, form: &str, out: &str) -> (bool, String, String) {
		let ctx = self.ctx();
		let code = self.statement_code(&ctx, form);
		self.statement_with(key, form, out, &code)
	}
	/// `sign-statement` typing `code`, under the current context.
	pub(crate) fn statement_with(
		&mut self,
		key: &str,
		form: &str,
		out: &str,
		code: &str,
	) -> (bool, String, String) {
		let ctx = self.ctx();
		self.io.say(&[PRIMARY, &code]);
		let (d, m) = (self.d(), self.fx.manifest());
		let r = self.io.run(&[
			"sign-statement",
			"--dir",
			&d,
			"--key",
			key,
			"--context",
			&ctx,
			"--trusted-manifest",
			&m,
			"--action",
			"grant",
			"--permission-key",
			&format!("{}:3", "4a".repeat(16)),
			"--sidecar",
			form,
			"--out",
			out,
		]);
		self.io.secrets.clear();
		r
	}
	/// `build statement`'s code for the fixture permission.
	pub(crate) fn statement_code(&mut self, ctx: &str, form: &str) -> String {
		let (d, m) = (self.d(), self.fx.manifest());
		let out = self.io.ok(&[
			"build",
			"statement",
			"--dir",
			&d,
			"--context",
			ctx,
			"--trusted-manifest",
			&m,
			"--action",
			"grant",
			"--permission-key",
			&format!("{}:3", "4a".repeat(16)),
			"--sidecar",
			form,
		]);
		out.trim()
			.strip_prefix("statement code ")
			.unwrap()
			.to_string()
	}
	pub(crate) fn read(&self, name: &str) -> Vec<u8> {
		std::fs::read(self.fx.keys.join(name)).unwrap()
	}
	pub(crate) fn written(&self, out: &str) -> bool {
		self.fx.exists(&format!("{out}.sidecar")) || self.fx.exists(&format!("{out}.command"))
	}
	/// Plants a Bound key file built outside the tool (for refusals no tool path can produce).
	pub(crate) fn plant(
		&self,
		name: &str,
		seed: [u8; 32],
		role: u8,
		epoch: u64,
		context: [u8; 188],
	) {
		let header = Header {
			state: State::Bound,
			log_n: NEW_LOG_N,
			p: NEW_P,
			salt: [0x5a; 32],
			nonce: [0x5b; 12],
			role,
			epoch,
			context: Some(context),
			key: public(&seed),
			backup_of: None,
		};
		let mut s = key_guard::custody::SecretBuf::zeroed(32);
		s.as_mut_slice().copy_from_slice(&seed);
		let mut p = key_guard::custody::SecretBuf::with_capacity(PRIMARY.len());
		PRIMARY.bytes().for_each(|b| p.push(b).unwrap());
		let file = crate::envelope::seal(&header, &s, &p).unwrap();
		let dir =
			key_guard::custody::open_custody_dir_anchored(&self.fx.keys, &self.fx.root).unwrap();
		dir.lock().unwrap().create_file(name, &file).unwrap();
	}
}

/// The code `sign` asks for: a rotation's new key prefix, otherwise the digest prefix.
pub(crate) fn code(c: &Command2) -> String {
	match c {
		Command2::RotateAdmin(k) | Command2::RotateGovernance(k) | Command2::RotateRecovery(k) => {
			hex(k)[..32].to_string()
		}
		_ => hex(&c.digest(GENERATION, CLUSTER).unwrap())[..32].to_string(),
	}
}

/// The nine governance kinds, with fields of this fixture's deployment.
pub(crate) fn nine() -> Vec<Command2> {
	let authority = AuthorityRef2 {
		id: [0x32; 16],
		revision: 1,
	};
	let lineage = [0x35; 16];
	vec![
		Command2::EnrollSelector(ProofKeyBinding {
			generation: GENERATION,
			tenant: 1,
			key_id: [0x31; 16],
			key_version: 1,
		}),
		Command2::EnrollAuthority {
			authority,
			node: NodeRef2 {
				node: 7,
				incarnation: [0x33; 16],
			},
			key: public(&[0x34; 32]),
		},
		Command2::RevokeAuthority(authority),
		Command2::Grant {
			lineage,
			incarnation: [0x36; 16],
			lease: SignedLease2 {
				epoch: 1,
				holder: 7,
				range_start: 0,
				range_end: 10,
				start: Hlc2 {
					physical_ms: 1,
					logical: 0,
					node: 7,
				},
				expiry: Hlc2 {
					physical_ms: 2,
					logical: 0,
					node: 7,
				},
				min_commit_index: 0,
				issuer: 7,
				signature: [0x3a; 64],
			},
			predecessor: None,
		},
		Command2::Release {
			lineage,
			latest: GrantRef2 {
				lineage,
				log: log(3),
				digest: [0x37; 32],
			},
		},
		Command2::Cut {
			expected: 1,
			next: 2,
			configuration: [0x61; 32],
		},
		Command2::Resolve {
			identity: MutationIdentity {
				generation: GENERATION,
				tenant: 1,
				scope: MutationScope::Raw,
				table: [0; 16],
				schema_generation: 0,
				caller: [0x38; 16],
				sequence: 1,
			},
			digest: [0x71; 32],
			reservation: log(3),
			cause: 1,
			anchor: log(2),
		},
		Command2::EnrollLeaseIssuer {
			node: 7,
			key: public(&[0x39; 32]),
		},
		Command2::RevokeLeaseIssuer { node: 7 },
	]
}

/// The output passes the facade's own check, independently of the tool.
pub(crate) fn accepted(r: &Root, command: &[u8], out: &str) {
	let sidecar = r.read(&format!("{out}.sidecar"));
	assert_eq!(
		r.read(&format!("{out}.command")),
		command,
		"the written command is the input"
	);
	facade::self_check(&r.verified(), command, Some(&sidecar)).unwrap();
}
