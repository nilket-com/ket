use super::*;
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
		let old = wire::Fixture::new("b");
		let mut base = old.any.admission().clone();
		base.admin_key = key(11);
		let m = AdminManifestThree3::new(
			base,
			GovernanceRole2::new([0xd2; 16], key(12)).unwrap(),
			RecoveryRole3::new([0xd3; 16], key(13)).unwrap(),
		)
		.unwrap()
		.encode()
		.unwrap()
		.to_vec();
		Self {
			f: wire::Fixture::from_manifest(m),
		}
	}
	pub(super) fn root(&self) -> &Root2 {
		root_and_origin2(&self.f.state).unwrap().0
	}
	pub(super) fn proof(&self, c: &Command2, roles: [u8; 2]) -> Vec<u8> {
		let admin = install_admin2(&self.f.state, &self.f.pin).unwrap();
		let mut b = vec![0; 224];
		b[..8].copy_from_slice(b"VISROL03");
		b[8..10].copy_from_slice(&1u16.to_le_bytes());
		b[12..44].copy_from_slice(&role_command::context_digest(&admin));
		b[44..76].copy_from_slice(&c.digest(self.f.dep.generation, self.f.dep.cluster).unwrap());
		for (i, at) in [76, 150].into_iter().enumerate() {
			let role = roles[i];
			b[at] = role;
			if role != 0 {
				b[at + 1..at + 9].copy_from_slice(
					&active_role3(&admin, usize::from(role - 1))
						.unwrap()
						.0
						.to_le_bytes(),
				);
				b[at + 9] = 1;
			}
		}
		for (i, at) in [76, 150].into_iter().enumerate() {
			if roles[i] != 0 {
				let form = match c {
					Command2::RetireRoot => role_command::RoleForm3::Retire,
					Command2::Freeze(_) => role_command::RoleForm3::Freeze,
					_ => role_command::RoleForm3::Legacy,
				};
				if let Ok(pre) = role_command::preimage_for(&b, roles, i, form) {
					let sig =
						SigningKey::from_bytes(&[[11, 12, 13][usize::from(roles[i] - 1)]; 32])
							.sign(&pre)
							.to_bytes();
					b[at + 10..at + 74].copy_from_slice(&sig);
				}
			}
		}
		b
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
		let old = self.legacy_statement();
		let mut b = [0; 345];
		b[..273].copy_from_slice(&old[..273]);
		b[..8].copy_from_slice(b"VISADS03");
		b[273..281].copy_from_slice(
			&active_role3(&install_admin2(&self.f.state, &self.f.pin).unwrap(), 0)
				.unwrap()
				.0
				.to_le_bytes(),
		);
		let sig = SigningKey::from_bytes(&[11; 32])
			.sign(&epoch_statement_preimage3(&b).unwrap())
			.to_bytes();
		b[281..].copy_from_slice(&sig);
		b
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
