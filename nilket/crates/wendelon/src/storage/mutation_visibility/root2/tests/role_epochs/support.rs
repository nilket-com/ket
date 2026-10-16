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
			Command2::RotateAdmin(_) | Command2::RotateGovernance(_) => [3, 0],
			Command2::RotateRecovery(_) => [1, 2],
			_ => [2, 0],
		}
	}
	pub(super) fn proof(&self, c: &Command2) -> Vec<u8> {
		self.proof_as(c, Self::expected(c))
	}
	pub(super) fn proof_as(&self, c: &Command2, roles: [u8; 2]) -> Vec<u8> {
		let admin = self.installed();
		let mut b = vec![0; 224];
		b[..8].copy_from_slice(b"VISROL03");
		b[8..10].copy_from_slice(&1u16.to_le_bytes());
		b[12..44].copy_from_slice(&role_command::context_digest(&admin));
		b[44..76].copy_from_slice(&c.digest(self.f.dep.generation, self.f.dep.cluster).unwrap());
		for (i, at) in [76, 150].into_iter().enumerate() {
			b[at] = roles[i];
			if roles[i] != 0 {
				b[at + 1..at + 9].copy_from_slice(
					&active_role3(&admin, usize::from(roles[i] - 1))
						.unwrap()
						.0
						.to_le_bytes(),
				);
				b[at + 9] = 1;
			}
		}
		for (i, at) in [76, 150].into_iter().enumerate() {
			if roles[i] != 0 {
				let sig = match role_command::preimage(&b, roles, i) {
					Ok(preimage) => {
						SigningKey::from_bytes(&[self.seeds[usize::from(roles[i] - 1)]; 32])
							.sign(&preimage)
							.to_bytes()
					}
					Err(role_command::RoleProofError3::Domain) => [0; 64],
					Err(error) => panic!("malformed test proof: {error:?}"),
				};
				b[at + 10..at + 74].copy_from_slice(&sig);
			}
		}
		b
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
		let old = self.legacy_statement();
		let mut b = [0; 345];
		b[..273].copy_from_slice(&old[..273]);
		b[..8].copy_from_slice(b"VISADS03");
		b[273..281].copy_from_slice(&active_role3(&self.installed(), 0).unwrap().0.to_le_bytes());
		let sig = SigningKey::from_bytes(&[self.seeds[0]; 32])
			.sign(&epoch_statement_preimage3(&b).unwrap())
			.to_bytes();
		b[281..].copy_from_slice(&sig);
		b
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
pub(super) fn epoch_for(a: &InstalledAdmin2, seed: u8, epoch: u64) -> [u8; 345] {
	let ctx = admin_context2(a);
	let ns = admin_namespace2(a);
	let k = PermissionKey2 {
		authority: wire::authority(),
		namespace: AdmissionNamespace2 {
			generation: ctx.generation,
			tenant: ns.tenant,
			scope: ns.scope,
			table: ns.table,
			schema: ns.schema,
		},
	};
	let old = encode_admin_statement2(AdminAction2::Grant, &k, &ctx, &[0; 64]).unwrap();
	let mut b = [0; 345];
	b[..273].copy_from_slice(&old[..273]);
	b[..8].copy_from_slice(b"VISADS03");
	b[273..281].copy_from_slice(&epoch.to_le_bytes());
	let sig = SigningKey::from_bytes(&[seed; 32])
		.sign(&epoch_statement_preimage3(&b).unwrap())
		.to_bytes();
	b[281..].copy_from_slice(&sig);
	b
}
