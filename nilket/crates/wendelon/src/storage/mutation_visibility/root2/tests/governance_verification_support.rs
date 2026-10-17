//! Test-only real-entry setup; no production capability or token constructors.
use crate::consensus::{Root2Carrier1, WendelonRequest, WendelonTypeConfig};
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AdminManifestTwo2, AnyAdminManifest2,
};
use crate::storage::mutation_visibility::root2::admin::record::BootstrapRecord2;
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::admin_statement::*;
use crate::storage::mutation_visibility::root2::auth::preimage::admission_preimage2;
use crate::storage::mutation_visibility::root2::command::{ApplyResult2, Command2};
use crate::storage::mutation_visibility::root2::committed::{CommittedSource2, committed_entry2};
use crate::storage::mutation_visibility::root2::governance_command::*;
use crate::storage::mutation_visibility::root2::lease_payload::lease_payload2;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::inventory::{
	LogState2, RootEntry2, RootIdentity2, RootState2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::encode_snapshot2;
use crate::storage::mutation_visibility::root2::recovery::Deployment2;
use crate::storage::mutation_visibility::root2::tests::{profile_two_binding, support};
use crate::storage::mutation_visibility::root2::trust::{TrustedEntry2, VerifiedCommand2};
use crate::storage::mutation_visibility::root2::types::*;
use crate::storage::mutation_visibility::root2::verification::{VerifyError2, advance_verified2};
use crate::storage::mutation_visibility::{
	MutationIdentity, MutationOperation, MutationScope, ProofKeyBinding,
};
use ed25519_dalek::{Signer, SigningKey};
use openraft::{BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership};
use std::collections::BTreeMap;

pub(in crate::storage::mutation_visibility::root2) type E = Entry<WendelonTypeConfig>;
pub(in crate::storage::mutation_visibility::root2) const ISSUER_SEED: u8 = 0x33;
pub(in crate::storage::mutation_visibility::root2) const AUTHORITY_SEED: u8 = 0x44;
pub(in crate::storage::mutation_visibility::root2) fn key(seed: u8) -> Digest2 {
	SigningKey::from_bytes(&[seed; 32])
		.verifying_key()
		.to_bytes()
}
pub(in crate::storage::mutation_visibility::root2) fn log(index: u64) -> Log2 {
	Log2 {
		term: 1,
		node: 1,
		index,
	}
}
pub(in crate::storage::mutation_visibility::root2) fn entry(index: u64) -> E {
	E {
		log_id: if index == 0 {
			LogId::default()
		} else {
			LogId::new(CommittedLeaderId::new(1, 1), index)
		},
		payload: if index == 0 {
			EntryPayload::Membership(Membership::from(BTreeMap::from([(
				1,
				BasicNode { addr: "s3".into() },
			)])))
		} else {
			EntryPayload::Blank
		},
	}
}
pub(in crate::storage::mutation_visibility::root2) fn carrier(
	command: &Command2,
	dep: Deployment2,
	index: u64,
	sidecar: Option<&[u8]>,
) -> E {
	let mut e = entry(index);
	e.payload = EntryPayload::Normal(WendelonRequest::Root2Command(
		Root2Carrier1::new(
			&command.encode(dep.generation, dep.cluster).unwrap(),
			sidecar,
		)
		.unwrap(),
	));
	e
}
pub(in crate::storage::mutation_visibility::root2) fn bytes(
	s: &MutationAuthorityState2,
) -> Vec<u8> {
	encode_authority2(s).unwrap()
}
pub(in crate::storage::mutation_visibility::root2) fn tip(s: &MutationAuthorityState2) -> Log2 {
	root_and_origin2(s).unwrap().0.covered()
}
pub(in crate::storage::mutation_visibility::root2) struct Fixture {
	pub(in crate::storage::mutation_visibility::root2) manifest: Vec<u8>,
	pub(in crate::storage::mutation_visibility::root2) any: AnyAdminManifest2,
	pub(in crate::storage::mutation_visibility::root2) record: BootstrapRecord2,
	pub(in crate::storage::mutation_visibility::root2) pin: PinnedBoundTuple2,
	pub(in crate::storage::mutation_visibility::root2) dep: Deployment2,
	pub(in crate::storage::mutation_visibility::root2) state: MutationAuthorityState2,
	pub(in crate::storage::mutation_visibility::root2) entries: Vec<E>,
}
impl Fixture {
	pub(in crate::storage::mutation_visibility::root2) fn new(profile: &str) -> Self {
		Self::from_manifest(profile_two_binding::manifest_bytes(profile))
	}
	pub(in crate::storage::mutation_visibility::root2) fn table_namespace() -> Self {
		let AnyAdminManifest2::Two(m) =
			AnyAdminManifest2::decode(&profile_two_binding::manifest_bytes("b")).unwrap()
		else {
			unreachable!()
		};
		let mut base = m.base().clone();
		base.namespace.scope = MutationScope::Table;
		base.namespace.table = [0x51; 16];
		base.namespace.schema = 7;
		let manifest = AdminManifestTwo2::new(base, m.governance())
			.unwrap()
			.encode()
			.unwrap()
			.to_vec();
		Self::from_manifest(manifest)
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
			control: profile_two_binding::exact_pin(&manifest),
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
		let chain = profile_two_binding::chain_of(&manifest, manifest.len() as u64);
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
	pub(in crate::storage::mutation_visibility::root2) fn envelope(
		&self,
		state: &MutationAuthorityState2,
		command: &Command2,
		seed: u8,
	) -> Vec<u8> {
		let ctx = admin_context2(&install_admin2(state, &self.pin).unwrap());
		sign_context(
			ctx,
			command
				.digest(self.dep.generation, self.dep.cluster)
				.unwrap(),
			seed,
		)
	}
	pub(in crate::storage::mutation_visibility::root2) fn apply(
		&mut self,
		command: Command2,
		sidecar: Option<Vec<u8>>,
	) -> ApplyResult2 {
		let index = tip(&self.state).index + 1;
		let e = carrier(&command, self.dep, index, sidecar.as_deref());
		let (next, out) = step(
			&self.state,
			&self.pin,
			self.dep,
			&e,
			&command,
			sidecar.as_deref(),
		)
		.unwrap();
		self.state = next;
		self.entries.push(e);
		out
	}
	pub(in crate::storage::mutation_visibility::root2) fn governed(
		&mut self,
		command: Command2,
	) -> ApplyResult2 {
		let envelope = self.envelope(&self.state, &command, 0x92);
		self.apply(command, Some(envelope))
	}
}
pub(in crate::storage::mutation_visibility::root2) fn tuple_for(
	manifest: &[u8],
	record: BootstrapRecord2,
) -> PinnedBoundTuple2 {
	let any = AnyAdminManifest2::decode(manifest).unwrap();
	let a = any.admission();
	let pin = profile_two_binding::exact_pin(manifest);
	let snapshot = encode_snapshot2(&LogState2 {
		roots: vec![RootEntry2 {
			terminal: None,
			identity: RootIdentity2 {
				generation: a.generation,
				cluster: a.cluster,
				pin,
				admin_key: a.admin_key,
			},
			manifest: any,
			state: RootState2::Bound,
			last_seq: record.inventory_seq,
			record: Some(record),
		}],
		last_seq: record.inventory_seq,
	})
	.unwrap();
	let ip = crate::storage::mutation_visibility::root2::codec::hash(b"", &snapshot);
	validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(manifest),
		manifest_pin: Some(&pin),
		snapshot: Some(&snapshot),
		inventory_pin: Some(&ip),
	})
	.unwrap()
}
pub(in crate::storage::mutation_visibility::root2) fn sign_context(
	ctx: AdminContext2,
	digest: Digest2,
	seed: u8,
) -> Vec<u8> {
	let unsigned = encode_governance_command2(&ctx, digest, &[0; 64]).unwrap();
	let signature = SigningKey::from_bytes(&[seed; 32])
		.sign(&governance_command_preimage2(&unsigned).unwrap())
		.to_bytes();
	encode_governance_command2(&ctx, digest, &signature)
		.unwrap()
		.to_vec()
}
pub(in crate::storage::mutation_visibility::root2) fn step(
	state: &MutationAuthorityState2,
	pin: &PinnedBoundTuple2,
	dep: Deployment2,
	e: &E,
	command: &Command2,
	sidecar: Option<&[u8]>,
) -> Result<(MutationAuthorityState2, ApplyResult2), VerifyError2> {
	let ce = committed_entry2(&CommittedSource2::synthetic(), e)
		.unwrap()
		.into_committed();
	let trusted = TrustedEntry2::bind(&ce, command, dep.generation, dep.cluster).unwrap();
	advance_verified2(state, pin, &ce, &trusted, command, sidecar)
}
/// FIXTURE-authorized CURRENT-REDUCER differential only; not the frozen independent S1c oracle.
pub(in crate::storage::mutation_visibility::root2) fn differential(
	f: &mut Fixture,
	command: Command2,
) -> ApplyResult2 {
	let envelope = f.envelope(&f.state, &command, 0x92);
	let e = carrier(&command, f.dep, tip(&f.state).index + 1, Some(&envelope));
	let ce = committed_entry2(&CommittedSource2::synthetic(), &e)
		.unwrap()
		.into_committed();
	let trusted = TrustedEntry2::bind(&ce, &command, f.dep.generation, f.dep.cluster).unwrap();
	let authorized =
		VerifiedCommand2::fixture(f.dep.generation, f.dep.cluster, &command, &[], 0).unwrap();
	let expected = advance_root2(&f.state, &trusted, &authorized, &command).unwrap();
	let before = bytes(&f.state);
	let out = f.apply(command, Some(envelope));
	assert!(out == expected.1, "current reducer differential result");
	assert_eq!(bytes(&f.state), bytes(&expected.0));
	assert_ne!(bytes(&f.state), before);
	out
}
pub(in crate::storage::mutation_visibility::root2) fn denied(
	f: &Fixture,
	command: &Command2,
	sidecar: &[u8],
) {
	let before = bytes(&f.state);
	let e = carrier(command, f.dep, tip(&f.state).index + 1, Some(sidecar));
	let (next, out) = step(&f.state, &f.pin, f.dep, &e, command, Some(sidecar)).unwrap();
	assert!(matches!(
		out,
		ApplyResult2::Denied {
			reason: 8,
			reference: None
		}
	));
	let root = root_and_origin2(&next).unwrap().0;
	assert_eq!(
		bytes(&next),
		bytes(&with_tip_for_test(
			&f.state,
			root.state.covered,
			root.state.fingerprint,
			root.state.command_digest
		))
	);
	assert_eq!(bytes(&f.state), before);
}
pub(in crate::storage::mutation_visibility::root2) fn selector(f: &Fixture) -> ProofKeyBinding {
	let a = f.any.admission();
	ProofKeyBinding {
		generation: a.generation,
		tenant: a.namespace.tenant,
		key_id: [0x42; 16],
		key_version: 1,
	}
}
pub(in crate::storage::mutation_visibility::root2) fn authority() -> AuthorityRef2 {
	AuthorityRef2 {
		id: [0x41; 16],
		revision: 1,
	}
}
pub(in crate::storage::mutation_visibility::root2) fn enroll_authority() -> Command2 {
	let c = support::claim();
	Command2::EnrollAuthority {
		authority: authority(),
		node: NodeRef2 {
			node: c.original.lease.holder,
			incarnation: c.original.incarnation,
		},
		key: key(AUTHORITY_SEED),
	}
}
pub(in crate::storage::mutation_visibility::root2) fn signed_lease(
	f: &Fixture,
	seed: u8,
) -> SignedLease2 {
	let mut lease = support::claim().original.lease;
	lease.epoch = f.any.admission().initial_epoch;
	lease.issuer = 7;
	lease.signature = SigningKey::from_bytes(&[seed; 32])
		.sign(&lease_payload2(&lease).unwrap())
		.to_bytes();
	lease
}
pub(in crate::storage::mutation_visibility::root2) fn grant(f: &Fixture, seed: u8) -> Command2 {
	let original = support::claim().original;
	Command2::Grant {
		lineage: original.lineage,
		incarnation: original.incarnation,
		lease: signed_lease(f, seed),
		predecessor: None,
	}
}
pub(in crate::storage::mutation_visibility::root2) fn identity(f: &Fixture) -> MutationIdentity {
	let a = f.any.admission();
	let n = a.namespace;
	MutationIdentity {
		generation: a.generation,
		tenant: n.tenant,
		scope: n.scope,
		table: n.table,
		schema_generation: n.schema,
		caller: [0x81; 16],
		sequence: 1,
	}
}
pub(in crate::storage::mutation_visibility::root2) fn nine(f: &Fixture) -> Vec<Command2> {
	let grant = grant(f, ISSUER_SEED);
	let Command2::Grant {
		lineage,
		incarnation,
		lease,
		predecessor,
	} = grant.clone()
	else {
		unreachable!()
	};
	let original = OriginalGrant2 {
		lineage,
		incarnation,
		lease,
		predecessor,
		log: log(3),
	};
	vec![
		Command2::EnrollSelector(selector(f)),
		enroll_authority(),
		Command2::RevokeAuthority(authority()),
		grant,
		Command2::Release {
			lineage,
			latest: original.reference().unwrap(),
		},
		Command2::Cut {
			expected: f.any.admission().initial_epoch,
			next: f.any.admission().initial_epoch + 1,
			configuration: [0x61; 32],
		},
		Command2::Resolve {
			identity: identity(f),
			digest: [0x71; 32],
			reservation: log(3),
			cause: 1,
			anchor: log(2),
		},
		Command2::EnrollLeaseIssuer {
			node: 7,
			key: key(ISSUER_SEED),
		},
		Command2::RevokeLeaseIssuer { node: 7 },
	]
}
pub(in crate::storage::mutation_visibility::root2) fn permission(
	f: &Fixture,
	claim: &ClaimProjection2,
) -> Command2 {
	let admin = install_admin2(&f.state, &f.pin).unwrap();
	let ctx = admin_context2(&admin);
	let id = claim.manifest.identity;
	let key = PermissionKey2 {
		authority: claim.authority,
		namespace: AdmissionNamespace2 {
			generation: id.generation,
			tenant: id.tenant,
			scope: id.scope,
			table: id.table,
			schema: id.schema_generation,
		},
	};
	let unsigned = encode_admin_statement2(AdminAction2::Grant, &key, &ctx, &[0; 64]).unwrap();
	let sig = SigningKey::from_bytes(&[0x42; 32])
		.sign(&admin_statement_preimage2(&unsigned).unwrap())
		.to_bytes();
	Command2::AdmissionPermission(
		encode_admin_statement2(AdminAction2::Grant, &key, &ctx, &sig).unwrap(),
	)
}
pub(in crate::storage::mutation_visibility::root2) fn prepared_claim(
	f: &Fixture,
) -> ClaimProjection2 {
	let mut c = support::claim();
	c.manifest.identity = identity(f);
	c.manifest.key_id = [0x42; 16];
	c.manifest.key_version = 1;
	c.manifest.operation = if c.manifest.identity.scope == MutationScope::Raw {
		MutationOperation::RawPut
	} else {
		MutationOperation::Insert
	};
	c.original = root_and_origin2(&f.state)
		.unwrap()
		.0
		.state
		.lineages
		.values()
		.next()
		.unwrap()
		.latest
		.clone();
	c.manifest.admission.epoch = c.original.lease.epoch;
	c.manifest.admission.holder = c.original.lease.holder;
	c.authority = authority();
	c.signature = SigningKey::from_bytes(&[AUTHORITY_SEED; 32])
		.sign(&admission_preimage2(&c, f.dep.cluster).unwrap())
		.to_bytes();
	c
}

pub(in crate::storage::mutation_visibility::root2) fn malformed_sidecar_error() -> VerifyError2 {
	VerifyError2::Governance(GovernanceCommandError2::Magic)
}
