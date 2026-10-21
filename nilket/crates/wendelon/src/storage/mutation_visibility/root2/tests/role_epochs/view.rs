//! T-VIEW: every profile 1/2/3 statement, governance and role-proof vector of these fixtures checks
//! to the same result through `view()` as it did through `InstalledAdmin2` before the checkers
//! took views: the goldens below were recorded by this matrix from the pre-view checkers. Each
//! view field, altered alone, changes at least one result.
use super::*;
use crate::storage::mutation_visibility::root2::governance_command::{
	GovernanceAction2, check_governance_command2,
};
use crate::storage::mutation_visibility::root2::role_view::RoleView3;
use role_command::RoleForm3;

/// Inputs fixed before any check, so a view decides only the results.
#[derive(Default)]
struct Inputs {
	statements: Vec<[u8; 337]>,
	epoch_statements: Vec<[u8; 345]>,
	governance: Vec<(GovernanceAction2, Vec<u8>, Digest2)>,
	proofs: Vec<(Vec<u8>, Digest2, [u8; 2], RoleForm3)>,
}
fn results(a: &RoleView3, i: &Inputs) -> Vec<String> {
	let mut out = Vec::new();
	for s in &i.statements {
		out.push(format!("{:?}", check_admin_statement2(a, s).map(|_| ())));
	}
	for s in &i.epoch_statements {
		out.push(format!("{:?}", check_epoch_statement3(a, s).map(|_| ())));
	}
	for (action, bytes, digest) in &i.governance {
		out.push(format!(
			"{:?}",
			check_governance_command2(a, *action, bytes, *digest).map(|_| ())
		));
	}
	for (bytes, digest, expected, form) in &i.proofs {
		out.push(format!(
			"{:?}",
			role_command::verify_core(a, bytes, *digest, *expected, *form)
		));
	}
	out
}
fn permission_key(a: &InstalledAdmin2) -> PermissionKey2 {
	let c = admin_context2(a);
	let ns = admin_namespace2(a);
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
fn statement(a: &InstalledAdmin2, seed: u8) -> [u8; 337] {
	let (k, c) = (permission_key(a), admin_context2(a));
	let unsigned = encode_admin_statement2(AdminAction2::Grant, &k, &c, &[0; 64]).unwrap();
	let sig = SigningKey::from_bytes(&[seed; 32])
		.sign(&admin_statement_preimage2(&unsigned).unwrap())
		.to_bytes();
	encode_admin_statement2(AdminAction2::Grant, &k, &c, &sig).unwrap()
}
fn flipped<const N: usize>(mut b: [u8; N], at: usize) -> [u8; N] {
	b[at] ^= 1;
	b
}
/// Statement, governance and role-proof inputs for the state `f` is in, with signer seeds
/// `admin` and `governance` and one wrong seed each.
fn add(
	i: &mut Inputs,
	f: &wire::Fixture,
	a: &InstalledAdmin2,
	seeds: [u8; 3],
	proof: &dyn Fn(&Command2, [u8; 2]) -> Vec<u8>,
) {
	let s = statement(a, seeds[0]);
	i.statements
		.extend([s, flipped(s, 300), flipped(s, 20), statement(a, 0x77)]);
	let epoch = active_role3(a, 0).map_or(0, |(e, _)| e);
	let e = epoch_for(a, seeds[0], epoch);
	i.epoch_statements.extend([
		e,
		flipped(e, 300),
		epoch_for(a, seeds[0], epoch + 1),
		epoch_for(a, 0x77, epoch),
	]);
	let (g, c) = (f.dep.generation, f.dep.cluster);
	let enroll = wire::enroll_authority();
	let d = enroll.digest(g, c).unwrap();
	let ctx = admin_context2(a);
	for (action, seed, digest) in [
		(GovernanceAction2::EnrollAuthority, seeds[1], d),
		(GovernanceAction2::Grant, seeds[1], d),
		(GovernanceAction2::EnrollAuthority, 0x77, d),
		(GovernanceAction2::EnrollAuthority, seeds[1], [9; 32]),
	] {
		i.governance
			.push((action, wire::sign_context(ctx, d, seed), digest));
	}
	for (command, roles, form) in [
		(enroll.clone(), [2, 0], RoleForm3::Legacy),
		(Command2::RotateAdmin(key(50)), [3, 0], RoleForm3::Legacy),
		(Command2::RotateRecovery(key(52)), [1, 2], RoleForm3::Legacy),
		(Command2::RotateAdmin(key(50)), [1, 2], RoleForm3::Legacy),
	] {
		let p = proof(&command, roles);
		let d = command.digest(g, c).unwrap();
		i.proofs.push((p.clone(), d, roles, form));
		i.proofs.push((p.clone(), [9; 32], roles, form));
		let mut stale = p;
		stale[77] ^= 1;
		i.proofs.push((stale, d, roles, form));
	}
}
/// Profile 3 through each role's rotation: every input is checked under every later
/// administrator, so stale inputs are part of the matrix.
fn matrix(check: &dyn Fn(&InstalledAdmin2, &Inputs) -> Vec<String>) -> Vec<Vec<String>> {
	let mut out = Vec::new();
	let mut f = F::new();
	let mut i = Inputs::default();
	for rotation in [None, Some((0, 40)), Some((1, 41)), Some((2, 42))] {
		if let Some((role, seed)) = rotation {
			f.rotate(role, seed);
		}
		let a = f.installed();
		add(&mut i, &f.f, &a, f.seeds, &|c, r| f.proof_as(c, r));
		out.push(check(&a, &i));
	}
	out
}
/// One code per result: `o` Ok(()), `T`/`F` Ok(true)/Ok(false), else the error's initial.
fn code(r: &str) -> char {
	match r {
		"Ok(())" => 'o',
		"Ok(true)" => 'T',
		"Ok(false)" => 'F',
		"Err(Signature)" => 'S',
		"Err(Context)" => 'C',
		"Err(Digest)" => 'D',
		"Err(NoGovernance)" => 'N',
		_ => '?',
	}
}
fn coded(rows: Vec<Vec<String>>) -> Vec<String> {
	rows.into_iter()
		.map(|row| row.iter().map(|r| code(r)).collect())
		.collect()
}
/// Recorded from this matrix through the checkers as they took `&InstalledAdmin2`, before views.
const GOLDEN: [&str; 4] = [
	"oSCSoSSSooSDTFFTFFTFFTFF",
	"SSCSSSCSSSSSoSSSooSDooSDTFFTFFFFFFFFTFFTFFTFFTFF",
	"SSCSSSCSSSCSSSSSoSSSoSSSSSSDSSSDSSSDFFFTFFFFFFFFFFFTFFFFFFFFTFFTFFTFFTFF",
	"SSCSSSCSSSCSSSCSSSSSoSSSoSSSoSSSSSSDSSSDSSSDSSSDFFFFFFFFFFFFFFFFFFFFFFFFTFFFFFTFFTFFTFFTFFTFFTFF",
];

#[test]
fn t_view_every_vector_checks_identically_through_the_view() {
	assert_eq!(coded(matrix(&|a, i| results(&a.view(), i))), GOLDEN);
	// The installed wrapper and the view-based core agree on every role proof.
	matrix(&|a, i| {
		for (bytes, digest, expected, form) in &i.proofs {
			assert_eq!(
				role_command::verify_for(a, bytes, *digest, *expected, *form),
				role_command::verify_core(&a.view(), bytes, *digest, *expected, *form)
			);
		}
		Vec::new()
	});
}
#[test]
fn t_view_each_altered_field_changes_a_result() {
	type Alter = fn(RoleView3) -> RoleView3;
	let alterations: [(&str, Alter); 4] = [
		("admin_key", |v| v.with_admin_key(key(0x55))),
		("active[0]", |v| v.with_active0((0, key(0x55)))),
		("namespace", |v| {
			let ns = v.namespace();
			v.with_namespace(AdminNamespace2 {
				tenant: ns.tenant + 1,
				..ns
			})
		}),
		("governance", |v| v.with_governance(None)),
	];
	for (field, alter) in alterations {
		let altered = coded(matrix(&|a, i| results(&alter(a.view()), i)));
		assert_ne!(altered, GOLDEN, "{field}");
	}
}
