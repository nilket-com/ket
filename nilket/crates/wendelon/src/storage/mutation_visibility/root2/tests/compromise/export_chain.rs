//! C-S6b context export, content: T-X2 (the successor chain re-verifies), T-CTX (the one context
//! encoder) and T-FMT (the byte format). Every export is written by the CLI over a real WAL.
use super::reader::{CONTEXT_DOMAIN, Parsed, Record, Refusal, encode, parse, resealed, verify};
use super::support::*;
use crate::storage::mutation_visibility::root2::admin_install::{AdminContext2, admin_context2};
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::command::Command2;
use crate::storage::mutation_visibility::root2::role_command::{context_bytes, context_digest};
use crate::storage::mutation_visibility::root2::types::Log2;

/// The export the CLI writes for `r`'s whole history, labelled `node`.
pub(super) fn exported(r: &Root, node: u32) -> Vec<u8> {
	let dirs = r.write(None);
	let out = dirs.inputs.join("context.wrctxexp");
	let run = audit(
		&dirs,
		&[suspects(&[(ADMIN, 0)]), export_args(&out, node)].concat(),
	);
	assert_eq!(run.code, 0, "{}", run.stderr);
	std::fs::read(&out).unwrap()
}
fn log(index: u64) -> Log2 {
	super::wire::log(index)
}
/// Two rotations per role, interleaved, with a freeze interval between the rounds: each later
/// proof is signed by a successor key, so every record checks under the keys active before it.
fn rotated() -> Root {
	let mut r = Root::new();
	r.rotate(ADMIN, 21); // 3
	r.rotate(GOVERNANCE, 31); // 4
	r.rotate(RECOVERY, 41); // 5, jointly by admin 1 and governance 1
	r.freeze(); // 6, recovery 1
	r.unfreeze(); // 7
	r.rotate(ADMIN, 22); // 8, recovery 1
	r.rotate(GOVERNANCE, 32); // 9
	r.rotate(RECOVERY, 42); // 10, admin 2 and governance 2
	r
}
fn refused(p: &Parsed, manifest: &[u8], expected: Refusal) {
	assert_eq!(verify(&encode(p), manifest).err(), Some(expected));
}

#[test]
fn t_x2_every_successor_reverifies_and_each_replay_check_refuses() {
	let r = rotated();
	let bytes = exported(&r, 7);
	let m = r.manifest();
	let v = verify(&bytes, m).unwrap();
	assert_eq!(
		v.history,
		[
			(ADMIN, 1, key(21)),
			(GOVERNANCE, 1, key(31)),
			(RECOVERY, 1, key(41)),
			(ADMIN, 2, key(22)),
			(GOVERNANCE, 2, key(32)),
			(RECOVERY, 2, key(42)),
		]
	);
	assert_eq!(v.active, [(2, key(22)), (2, key(32)), (2, key(42))]);
	assert!(v.context_authenticated);
	// Audit-derived, readable only through the fail-safe accessors.
	assert!(!v.frozen.refuses_while_frozen());
	assert!(!v.retired.refuses_as_retired());
	assert_eq!(v.transition.freeze_binding(), log(7));
	let p = parse(&bytes).unwrap();
	assert_eq!(
		p.successors.iter().map(|s| s.activated).collect::<Vec<_>>(),
		[3, 4, 5, 8, 9, 10].map(log)
	);
	// Control: the test's own encoder reproduces the writer's bytes, so every refusal below is
	// the one edited field.
	assert_eq!(encode(&p), bytes);
	let (g, c) = (r.f.dep.generation, r.f.dep.cluster);
	// Record key is not the command's key.
	let mut x = p.clone();
	x.successors[0].key = key(99);
	refused(&x, m, Refusal::Key);
	// Record role is not the command kind.
	let mut x = p.clone();
	x.successors[0].role = GOVERNANCE;
	refused(&x, m, Refusal::Kind);
	// Epoch gap.
	let mut x = p.clone();
	x.successors[3].epoch = 3;
	refused(&x, m, Refusal::Epoch);
	// A duplicate key: a validly signed admin rotation to the original recovery key. Control: the
	// same splice to a fresh key verifies, so the refusal is the duplicate alone.
	let splice = |k| {
		let command = Command2::RotateAdmin(k);
		let mut x = p.clone();
		x.successors.truncate(1);
		x.successors[0] = Record {
			role: ADMIN,
			epoch: 1,
			key: k,
			activated: log(3),
			command: command.encode(g, c).unwrap(),
			sidecar: r.proof(&command, [(RECOVERY, 0, 13), (0, 0, 0)]),
		};
		x
	};
	assert!(verify(&encode(&splice(key(98))), m).is_ok());
	refused(&splice(key(13)), m, Refusal::Duplicate);
	// Out-of-order activations: the admin and governance successors swapped.
	let mut x = p.clone();
	x.successors.swap(0, 1);
	refused(&x, m, Refusal::Order);
	// An activation at or before the install entry.
	let mut x = p.clone();
	x.successors[0].activated = log(2);
	refused(&x, m, Refusal::Order);
	// (generation, cluster) mismatch: the same rotation under another generation.
	let mut x = p.clone();
	let mut other = g;
	other[0] ^= 0x01;
	x.successors[0].command = Command2::RotateAdmin(key(21)).encode(other, c).unwrap();
	refused(&x, m, Refusal::Deployment);
	// A context altered while count >= 1: every proof binds the context digest.
	let mut x = p.clone();
	x.context[120] ^= 0x01;
	refused(&x, m, Refusal::Proof);
	// A proof by the wrong signer (admin 0 instead of recovery 0) for the first record.
	let mut x = p.clone();
	x.successors[0].sidecar = r.proof(
		&Command2::RotateAdmin(key(21)),
		[(RECOVERY, 0, 11), (0, 0, 0)],
	);
	refused(&x, m, Refusal::Proof);
	// Not the trusted manifest.
	let mut other = m.to_vec();
	other[200] ^= 0x01;
	assert_eq!(verify(&bytes, &other).err(), Some(Refusal::Manifest));
}

#[test]
fn t_x2_count_zero_verifies_without_authenticating_the_context() {
	let mut r = Root::new();
	applied(r.governance_use(r.current(GOVERNANCE)));
	let bytes = exported(&r, 7);
	let v = verify(&bytes, r.manifest()).unwrap();
	assert!(v.history.is_empty());
	assert!(!v.context_authenticated);
	assert_eq!(v.active, [(0, key(11)), (0, key(12)), (0, key(13))]);
	// Why: with no successor nothing binds the context, so an altered one passes the chain too.
	let mut p = parse(&bytes).unwrap();
	p.context[120] ^= 0x01;
	let altered = verify(&encode(&p), r.manifest()).unwrap();
	assert!(!altered.context_authenticated);
	// Control: one successor binds it.
	r.rotate(ADMIN, 21);
	let one = exported(&r, 7);
	assert!(verify(&one, r.manifest()).unwrap().context_authenticated);
	let mut p = parse(&one).unwrap();
	p.context[120] ^= 0x01;
	assert_eq!(
		verify(&encode(&p), r.manifest()).err(),
		Some(Refusal::Proof)
	);
}

/// The encoder `context_digest` hashed before C-S6b, verbatim: the no-behaviour-change reference.
fn previous_digest(c: &AdminContext2) -> [u8; 32] {
	let mut b = Vec::with_capacity(188);
	b.extend(c.generation);
	b.extend(c.cluster);
	b.extend(c.installed_at.term.to_le_bytes());
	b.extend(c.installed_at.node.to_le_bytes());
	b.extend(c.installed_at.index.to_le_bytes());
	b.extend(c.bootstrap_fingerprint);
	b.extend(c.bootstrap_command);
	b.extend(c.genesis);
	b.extend(c.initial_epoch.to_le_bytes());
	b.extend(c.manifest_pin);
	hash(b"VISROOT2-ROLE3-CONTEXT\0", &b)
}
fn hex(bytes: &[u8]) -> String {
	bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn t_ctx_the_exported_context_is_the_digested_context() {
	let r = Root::new();
	let p = parse(&exported(&r, 7)).unwrap();
	let base = admin_context2(r.admin());
	assert_eq!(p.context, context_bytes(&base));
	assert_eq!(context_digest(r.admin()), hash(CONTEXT_DOMAIN, &p.context));
	assert_eq!(context_digest(r.admin()), previous_digest(&base));
	assert_eq!(hex(&context_digest(r.admin())), GOLDEN_CONTEXT_DIGEST);
	// Each of the eight fields in turn (the install LogId in each of its three parts) changes the
	// bytes and the digest, and the digest still matches the previous encoder.
	type Edit = fn(&mut AdminContext2);
	let edits: [Edit; 10] = [
		|c| c.generation[15] ^= 1,
		|c| c.cluster[0] ^= 1,
		|c| c.installed_at.term += 1,
		|c| c.installed_at.node += 1,
		|c| c.installed_at.index += 1,
		|c| c.bootstrap_fingerprint[31] ^= 1,
		|c| c.bootstrap_command[0] ^= 1,
		|c| c.genesis[7] ^= 1,
		|c| c.initial_epoch += 1,
		|c| c.manifest_pin[0] ^= 1,
	];
	for (i, edit) in edits.iter().enumerate() {
		let mut x = base;
		edit(&mut x);
		assert_ne!(context_bytes(&x), context_bytes(&base), "field edit {i}");
		let digest = hash(CONTEXT_DOMAIN, &context_bytes(&x));
		assert_ne!(digest, hash(CONTEXT_DOMAIN, &p.context), "field edit {i}");
		assert_eq!(digest, previous_digest(&x), "field edit {i}");
	}
}
#[test]
fn t_ctx_context_bytes_match_the_independent_role_vectors() {
	// role_epochs/reference.py's synthetic context, built field by field; the vector's proof
	// embeds the digest of exactly these bytes.
	let v: serde_json::Value =
		serde_json::from_str(include_str!("../role_epochs/vectors.json")).unwrap();
	let context = AdminContext2 {
		generation: [1; 16],
		cluster: [2; 16],
		installed_at: Log2 {
			term: 1,
			node: 1,
			index: 2,
		},
		bootstrap_fingerprint: [b'B'; 32],
		bootstrap_command: [b'A'; 32],
		genesis: [b'G'; 32],
		initial_epoch: 7,
		manifest_pin: [b'P'; 32],
	};
	assert_eq!(hex(&context_bytes(&context)), v["context"]);
	let proof = v["proof"].as_str().unwrap();
	assert_eq!(
		proof[24..88],
		hex(&hash(CONTEXT_DOMAIN, &context_bytes(&context)))
	);
	// Control: one changed field no longer matches the vector.
	let mut other = context;
	other.initial_epoch = 8;
	assert_ne!(hex(&context_bytes(&other)), v["context"]);
}
/// `context_digest` of the fixture root, pinned (it also equals the previous encoder's digest).
const GOLDEN_CONTEXT_DIGEST: &str =
	"13dfbfeb036eaed7ab1a978436e1269b37262c7e2a05564374e29c111ac9bba6";

/// Rotation, a freeze interval, then retirement: one successor and both optional fields present.
fn retired() -> Root {
	let mut r = Root::new();
	r.rotate(ADMIN, 21); // 3
	r.freeze(); // 4
	r.unfreeze(); // 5
	r.retire(); // 6
	r
}

#[test]
fn t_fmt_golden_layout_and_refusals() {
	let r = retired();
	let bytes = exported(&r, 0x0102_0304);
	let p = parse(&bytes).unwrap();
	let s = &p.successors[0];
	// The fixed prefix, offset by offset.
	assert_eq!(&bytes[..8], b"WRCTXEXP");
	assert_eq!(bytes[8..12], [1, 0, 0, 0]);
	assert_eq!(bytes[12..200], context_bytes(&admin_context2(r.admin())));
	assert_eq!(bytes[200..441], *r.manifest());
	assert_eq!(bytes[441..445], [4, 3, 2, 1]);
	let stop = r.f.entries.last().unwrap().log_id;
	assert_eq!(bytes[445..453], stop.leader_id.term.to_le_bytes());
	assert_eq!(bytes[453..457], stop.leader_id.node_id.to_le_bytes());
	assert_eq!(bytes[457..465], stop.index.to_le_bytes());
	assert_eq!(bytes[465..467], [1, 0], "coverage complete, not frozen");
	assert_eq!(
		(bytes[467], bytes[488]),
		(1, 1),
		"transition and retired present"
	);
	assert_eq!(p.transition, Some(log(5)));
	assert_eq!(p.retired, Some(log(6)));
	assert_eq!(bytes[509..513], 1u32.to_le_bytes());
	assert_eq!(
		(s.role, s.epoch, s.key, s.activated),
		(ADMIN, 1, key(21), log(3))
	);
	let n = s.command.len();
	assert_eq!(bytes.len(), 513 + 1 + 8 + 32 + 20 + 4 + n + 4 + 224 + 32);
	assert_eq!(encode(&p), bytes);
	assert_eq!(hex(&hash(b"", &bytes)), GOLDEN_EXPORT_SHA256);
	// Refusals, each re-sealed so the edited field, not the digest, is what refuses.
	let edit = |at: usize, value: u8| {
		let mut b = bytes.clone();
		b[at] = value;
		parse(&resealed(&b)).err()
	};
	assert_eq!(edit(0, b'X'), Some(Refusal::Magic));
	assert_eq!(edit(8, 2), Some(Refusal::Version));
	assert_eq!(edit(10, 1), Some(Refusal::Reserved));
	assert_eq!(edit(465, 0), Some(Refusal::Coverage));
	assert_eq!(edit(465, 2), Some(Refusal::Coverage));
	assert_eq!(edit(466, 2), Some(Refusal::Boolean));
	assert_eq!(edit(467, 2), Some(Refusal::Boolean));
	assert_eq!(edit(488, 2), Some(Refusal::Boolean));
	assert_eq!(
		edit(467, 0),
		Some(Refusal::Fill),
		"absent with a non-zero LogId"
	);
	assert_eq!(edit(488, 0), Some(Refusal::Fill));
	let sidecar_len = 513 + 1 + 8 + 32 + 20 + 4 + n;
	assert_eq!(edit(sidecar_len, 223), Some(Refusal::SidecarLength));
	assert_eq!(edit(sidecar_len, 225), Some(Refusal::SidecarLength));
	assert_eq!(edit(513, 0), Some(Refusal::Role));
	assert_eq!(edit(513, 4), Some(Refusal::Role));
	let command_len = 513 + 1 + 8 + 32 + 20;
	let mut b = bytes.clone();
	b[command_len..command_len + 4].copy_from_slice(&262_193u32.to_le_bytes());
	assert_eq!(parse(&resealed(&b)).err(), Some(Refusal::CommandLength));
	// Trailing bytes before the digest.
	let mut b = bytes[..bytes.len() - 32].to_vec();
	b.push(0);
	assert_eq!(
		parse(&super::reader::reseal(b)).err(),
		Some(Refusal::Trailing)
	);
	// A wrong-width LogId (the stop one byte short, or one byte long) never parses.
	for width in [19usize, 21] {
		let mut b = bytes[..445].to_vec();
		b.extend(&bytes[445..445 + width.min(20)]);
		if width > 20 {
			b.push(0);
		}
		b.extend(&bytes[465..bytes.len() - 32]);
		assert!(parse(&super::reader::reseal(b)).is_err(), "width {width}");
	}
	// Counts over the caps, refused before any record is read.
	let mut b = bytes.clone();
	b[509..513].copy_from_slice(&193u32.to_le_bytes());
	assert_eq!(parse(&resealed(&b)).err(), Some(Refusal::Count));
	let mut x = p.clone();
	x.successors = vec![s.clone(); 65];
	assert_eq!(parse(&encode(&x)).err(), Some(Refusal::RoleCount));
	x.successors.truncate(64);
	assert!(parse(&encode(&x)).is_ok(), "64 per role is within the cap");
	// Integrity: an edit without a re-seal, and truncation.
	let mut b = bytes.clone();
	b[300] ^= 1;
	assert_eq!(parse(&b).err(), Some(Refusal::Digest));
	assert!(parse(&bytes[..bytes.len() - 1]).is_err());
	assert_eq!(parse(&bytes[..20]).err(), Some(Refusal::Truncated));
}
/// SHA-256 of the whole export of `retired()` labelled node 0x01020304, pinned.
const GOLDEN_EXPORT_SHA256: &str =
	"90844b5aac5816ea6c8c6b81446ead27893e3f5be120a0f0d1b9f6dbcae40281";
