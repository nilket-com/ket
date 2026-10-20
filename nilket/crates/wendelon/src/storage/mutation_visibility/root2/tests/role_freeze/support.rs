use super::*;
pub(super) fn key(seed: u8) -> Digest2 {
	SigningKey::from_bytes(&[seed; 32])
		.verifying_key()
		.to_bytes()
}
pub(super) fn deny(result: ApplyResult2, reason: u16) {
	assert!(
		result
			== ApplyResult2::Denied {
				reason,
				reference: None
			}
	);
}
pub(super) struct F {
	pub(super) f: wire::Fixture,
	pub(super) seeds: [u8; 3],
}
impl F {
	pub(super) fn new() -> Self {
		Self {
			f: wire::Fixture::profile_three(&crate::storage::mutation_visibility::root2::tests::profile_three_fixture::M3_PRIMARY),
			seeds: [11, 12, 13],
		}
	}
	pub(super) fn root(&self) -> &super::super::super::Root2 {
		root_and_origin2(&self.f.state).unwrap().0
	}
	pub(super) fn installed(&self) -> InstalledAdmin2 {
		install_admin2(&self.f.state, &self.f.pin).unwrap()
	}
	pub(super) fn command(role: usize, seed: u8) -> Command2 {
		match role {
			0 => Command2::RotateAdmin(key(seed)),
			1 => Command2::RotateGovernance(key(seed)),
			_ => Command2::RotateRecovery(key(seed)),
		}
	}
	pub(super) fn expected(c: &Command2) -> [u8; 2] {
		match c {
			Command2::RotateAdmin(_) | Command2::RotateGovernance(_) | Command2::Freeze(_) => {
				[3, 0]
			}
			Command2::Unfreeze(_) => [1, 3],
			Command2::RotateRecovery(_) => [1, 2],
			_ => [2, 0],
		}
	}
	pub(super) fn proof(&self, c: &Command2) -> Vec<u8> {
		self.proof_as(c, Self::expected(c))
	}
	pub(super) fn proof_as(&self, c: &Command2, roles: [u8; 2]) -> Vec<u8> {
		let admin = self.installed();
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
		let mut sigs = [[0; 64]; 2];
		for i in 0..2 {
			if roles[i] != 0 {
				sigs[i] = match role_command::preimage_for(
					&unsigned,
					roles,
					i,
					match c {
						Command2::Freeze(_) => role_command::RoleForm3::Freeze,
						Command2::Unfreeze(_) => role_command::RoleForm3::Unfreeze,
						_ => role_command::RoleForm3::Legacy,
					},
				) {
					Ok(preimage) => {
						SigningKey::from_bytes(&[self.seeds[usize::from(roles[i] - 1)]; 32])
							.sign(&preimage)
							.to_bytes()
					}
					Err(role_command::RoleProofError3::Domain) => [0; 64],
					Err(error) => panic!("malformed test proof: {error:?}"),
				};
			}
		}
		role_command::encode_role_proof3(context, digest, [slot(0, sigs[0]), slot(1, sigs[1])])
			.unwrap()
			.to_vec()
	}
	pub(super) fn apply(&mut self, c: Command2, proof: Vec<u8>) -> ApplyResult2 {
		self.f.apply(c, Some(proof))
	}
	pub(super) fn rotate(&mut self, role: usize, seed: u8) {
		let c = Self::command(role, seed);
		let p = self.proof(&c);
		let next = wire::tip(&self.f.state).index + 1;
		assert!(self.apply(c, p) == ApplyResult2::Registered(wire::log(next)));
		self.seeds[role] = seed;
	}
	pub(super) fn legacy_statement(&self) -> [u8; 337] {
		let c = admin_context2(&self.installed());
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
		let a = self.installed();
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
		let sig = SigningKey::from_bytes(&[self.seeds[0]; 32])
			.sign(&epoch_statement_preimage3(&unsigned).unwrap())
			.to_bytes();
		encode_epoch_statement3(AdminAction2::Grant, &k, &c, epoch, &sig).unwrap()
	}
	pub(super) fn enroll(&mut self) {
		let c = wire::enroll_authority();
		let p = self.proof(&c);
		assert!(matches!(self.apply(c, p), ApplyResult2::Registered(_)));
	}
	pub(super) fn roundtrip(&self) {
		let bytes = self.root().encode().unwrap();
		assert_eq!(
			super::super::super::UnverifiedRoot2::decode(&bytes)
				.unwrap()
				.encode()
				.unwrap(),
			bytes
		);
		assert!(
			super::super::super::validation::usage(&self.root().state).unwrap()
				== self.root().usage().unwrap()
		);
	}
}
pub(super) fn reseal(mut b: Vec<u8>) -> Vec<u8> {
	let end = b.len() - 32;
	let h = hash(b"", &b[..end]);
	b[end..].copy_from_slice(&h);
	b
}

impl F {
	pub(super) fn transition(&self) -> Log2 {
		self.root()
			.state
			.role_epochs
			.as_ref()
			.map_or(zero(), |e| e.transition)
	}
	pub(super) fn freeze(&mut self) {
		let c = Command2::Freeze(self.transition());
		let p = self.proof(&c);
		let index = wire::tip(&self.f.state).index + 1;
		assert!(self.apply(c, p) == ApplyResult2::Registered(wire::log(index)));
	}
	pub(super) fn unfreeze(&mut self, roles: [u8; 2]) {
		let c = Command2::Unfreeze(self.transition());
		let p = self.proof_as(&c, roles);
		let index = wire::tip(&self.f.state).index + 1;
		assert!(self.apply(c, p) == ApplyResult2::Registered(wire::log(index)));
	}
}
pub(super) fn zero() -> Log2 {
	Log2 {
		term: 0,
		node: 0,
		index: 0,
	}
}
