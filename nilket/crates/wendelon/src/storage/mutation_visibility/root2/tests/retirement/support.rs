use super::*;
use crate::storage::mutation_visibility::root2::tests::profile_three_fixture::M3_PRIMARY;
pub(super) fn key(seed: u8) -> Digest2 {
	SigningKey::from_bytes(&[seed; 32])
		.verifying_key()
		.to_bytes()
}
pub(super) struct F {
	pub(super) f: wire::Fixture,
}
impl F {
	pub(super) fn new() -> Self {
		Self {
			f: wire::Fixture::profile_three(&M3_PRIMARY),
		}
	}
	pub(super) fn root(&self) -> &Root2 {
		root_and_origin2(&self.f.state).unwrap().0
	}
	pub(super) fn proof(&self, c: &Command2, roles: [u8; 2]) -> Vec<u8> {
		let admin = install_admin2(&self.f.state, &self.f.pin).unwrap();
		let (context, digest) = (
			role_command::context_digest(&admin),
			c.digest(self.f.dep.generation, self.f.dep.cluster).unwrap(),
		);
		let slot = |i: usize, sig: [u8; 64]| match roles[i] {
			0 => (0, 0, false, sig),
			r => (
				r,
				active_role3(&admin, usize::from(r - 1)).unwrap().0,
				true,
				sig,
			),
		};
		let unsigned =
			role_command::encode_role_proof3(context, digest, [slot(0, [0; 64]), slot(1, [0; 64])])
				.unwrap();
		let form = match c {
			Command2::RetireRoot => role_command::RoleForm3::Retire,
			Command2::Freeze(_) => role_command::RoleForm3::Freeze,
			_ => role_command::RoleForm3::Legacy,
		};
		let mut sigs = [[0; 64]; 2];
		for i in 0..2 {
			if roles[i] != 0
				&& let Ok(pre) = role_command::preimage_for(&unsigned, roles, i, form)
			{
				sigs[i] = SigningKey::from_bytes(&[[11, 12, 13][usize::from(roles[i] - 1)]; 32])
					.sign(&pre)
					.to_bytes();
			}
		}
		role_command::encode_role_proof3(context, digest, [slot(0, sigs[0]), slot(1, sigs[1])])
			.unwrap()
			.to_vec()
	}
	pub(super) fn retire(&mut self, roles: [u8; 2]) {
		let c = Command2::RetireRoot;
		let p = self.proof(&c, roles);
		let at = wire::tip(&self.f.state).index + 1;
		assert!(self.f.apply(c, Some(p)) == ApplyResult2::Registered(wire::log(at)));
	}
	pub(super) fn freeze(&mut self) {
		let c = Command2::Freeze(Log2 {
			term: 0,
			node: 0,
			index: 0,
		});
		let p = self.proof(&c, [3, 0]);
		let _ = self.f.apply(c, Some(p));
	}
	pub(super) fn legacy_statement(&self) -> [u8; 337] {
		let c = admin_context2(&install_admin2(&self.f.state, &self.f.pin).unwrap());
		let ns = self.f.any.admission().namespace;
		let k = PermissionKey2 {
			authority: wire::authority(),
			namespace: AdmissionNamespace2 {
				generation: c.generation,
				tenant: ns.tenant,
				scope: ns.scope,
				table: ns.table,
				schema: ns.schema,
			},
		};
		let unsigned = encode_admin_statement2(AdminAction2::Grant, &k, &c, &[0; 64]).unwrap();
		let sig = SigningKey::from_bytes(&[11; 32])
			.sign(&admin_statement_preimage2(&unsigned).unwrap())
			.to_bytes();
		encode_admin_statement2(AdminAction2::Grant, &k, &c, &sig).unwrap()
	}
	pub(super) fn statement(&self) -> [u8; 345] {
		let a = install_admin2(&self.f.state, &self.f.pin).unwrap();
		let (c, ns) = (admin_context2(&a), admin_namespace2(&a));
		let k = PermissionKey2 {
			authority: wire::authority(),
			namespace: AdmissionNamespace2 {
				generation: c.generation,
				tenant: ns.tenant,
				scope: ns.scope,
				table: ns.table,
				schema: ns.schema,
			},
		};
		let epoch = active_role3(&a, 0).unwrap().0;
		let unsigned =
			encode_epoch_statement3(AdminAction2::Grant, &k, &c, epoch, &[0; 64]).unwrap();
		let sig = SigningKey::from_bytes(&[11; 32])
			.sign(&epoch_statement_preimage3(&unsigned).unwrap())
			.to_bytes();
		encode_epoch_statement3(AdminAction2::Grant, &k, &c, epoch, &sig).unwrap()
	}

	pub(super) fn terminal(&mut self, c: Command2, p: Option<Vec<u8>>) -> ApplyResult2 {
		self.f.apply(c, p)
	}
}
pub(super) fn denied(r: ApplyResult2, n: u16) {
	assert!(
		r == ApplyResult2::Denied {
			reason: n,
			reference: None
		}
	);
}
pub(super) fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
