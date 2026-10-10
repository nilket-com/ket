use super::*;
#[derive(Clone, PartialEq, Eq)]
pub struct UnverifiedWitness2 {
	pub claim: ClaimProjection2,
	pub reservation: Log2,
	pub receipts: Vec<UnverifiedReceipt2>,
	pub attestation: HolderAttestation2,
}
#[derive(Clone, PartialEq, Eq)]
pub struct UnverifiedOutcome2 {
	pub generation: [u8; 16],
	pub cluster: [u8; 16],
	pub identity: MutationIdentity,
	pub claim: Digest2,
	pub decision: Log2,
	pub result: Vec<u8>,
	pub response: Vec<u8>,
}
opaque!(UnverifiedWitness2, UnverifiedOutcome2);
// Receipt child-binding digests remain opaque/verifier-deferred (including literal golden
// placeholders). Structural decoding does not recompute child_binding2 or authenticate receipts.
// Later scoped integration must do both before treating a witness as durable or read-ready.
impl UnverifiedWitness2 {
	fn validate(&self) -> Result2<()> {
		check_claim(&self.claim)?;
		crate::storage::mutation_visibility::root2::codec::valid_log(self.reservation)?;
		self.attestation.encode()?;
		check(!self.receipts.is_empty() && self.receipts.len() <= 256 * 3)?;
		let digest = self.claim.digest()?;
		let original = self.claim.original.reference()?;
		check(
			self.attestation.grant == original
				&& self.attestation.reservation == self.reservation
				&& self.attestation.claim_digest == digest
				&& self.attestation.authority == self.claim.authority
				&& self.attestation.children.len() == self.claim.plans.len(),
		)?;
		let mut last = None;
		let cluster = self.receipts[0].cluster;
		for r in &self.receipts {
			r.encode()?;
			let key = (r.child, r.node);
			check(last.is_none_or(|p| p < key))?;
			last = Some(key);
			check(
				r.generation == claim_identity(&self.claim).generation
					&& r.cluster == cluster
					&& r.identity == claim_identity(&self.claim)
					&& r.claim == digest
					&& r.reservation == self.reservation
					&& r.original == original
					&& r.placement == self.claim.placement
					&& r.authority == self.claim.authority,
			)?;
			let plan = self
				.claim
				.plans
				.get(r.child as usize)
				.ok_or(RootError2::Codec)?;
			let ack = self
				.attestation
				.children
				.get(r.child as usize)
				.ok_or(RootError2::Codec)?;
			check(
				plan.child == r.child
					&& ack.child == r.child
					&& plan.nodes.contains(&r.node)
					&& ack.nodes.contains(&r.node)
					&& ack.durable_binding == r.binding,
			)?;
		}
		for (i, (plan, ack)) in self
			.claim
			.plans
			.iter()
			.zip(&self.attestation.children)
			.enumerate()
		{
			let nodes: Vec<_> = self
				.receipts
				.iter()
				.filter(|r| r.child as usize == i)
				.map(|r| r.node)
				.collect();
			check(
				nodes == ack.nodes
					&& nodes.len() >= plan.nodes.len() / 2 + 1
					&& nodes.iter().any(|n| {
						n.node == self.claim.manifest.admission.holder
							&& n.incarnation == self.claim.original.incarnation
					}),
			)?;
		}
		Ok(())
	}
	pub fn encode(&self) -> Result2<Vec<u8>> {
		self.validate()?;
		let claim = self.claim.encode()?;
		let att = self.attestation.encode()?;
		let size = 78usize
			.checked_add(claim.len())
			.and_then(|n| n.checked_add(att.len()))
			.and_then(|n| n.checked_add(self.receipts.len() * 390))
			.ok_or(RootError2::Overflow)?;
		if size > WITNESS_MAX {
			return Err(RootError2::Limit);
		}
		let mut b = Vec::new();
		blob(&mut b, &claim)?;
		log(&mut b, self.reservation)?;
		b.extend((self.receipts.len() as u16).to_le_bytes());
		for r in &self.receipts {
			blob(&mut b, &r.encode()?)?;
		}
		blob(&mut b, &att)?;
		wrap(b"VISWIT02", &b, WITNESS_MAX)
	}
	pub fn decode(bytes: &[u8]) -> Result2<Self> {
		let mut r = open(bytes, b"VISWIT02", WITNESS_MAX)?;
		let claim = ClaimProjection2::decode(r.blob(CLAIM_BYTES2)?)?;
		let reservation = r.log()?;
		let n = count16(&mut r, 256 * 3, 390)?;
		let mut receipts = Vec::new();
		for _ in 0..n {
			receipts.push(UnverifiedReceipt2::decode(r.blob(4096)?)?);
		}
		let attestation = HolderAttestation2::decode(r.blob(ATTESTATION_BYTES2)?)?;
		r.end()?;
		let v = Self {
			claim,
			reservation,
			receipts,
			attestation,
		};
		v.validate()?;
		Ok(v)
	}
}
impl UnverifiedOutcome2 {
	pub(super) fn terminal(&self) -> Result2<(u8, ParentRecord2)> {
		nonzero16(self.generation)?;
		nonzero16(self.cluster)?;
		if self.response.len() > 65536 {
			return Err(RootError2::Limit);
		}
		let (g, c, processed, result) = ApplyResult2::decode(&self.result)?;
		check(
			(g, c) == (self.generation, self.cluster)
				&& self.identity.generation == g
				&& self.decision.index <= processed.index,
		)?;
		let ApplyResult2::Parent { record, .. } = result else {
			return Err(RootError2::Codec);
		};
		check(
			record.claim.manifest.identity == self.identity && record.claim.digest()? == self.claim,
		)?;
		let t = record.terminal.ok_or(RootError2::Codec)?;
		check(t.first() == self.decision)?;
		Ok((
			match t {
				Terminal2::Accepted { .. } => 1,
				Terminal2::Refused { .. } => 2,
			},
			record,
		))
	}
	pub fn encode(&self) -> Result2<Vec<u8>> {
		let (kind, _) = self.terminal()?;
		let mut b = Vec::new();
		b.extend(self.generation);
		b.extend(self.cluster);
		put_id(&mut b, self.identity)?;
		b.extend(self.claim);
		log(&mut b, self.decision)?;
		blob(&mut b, &self.result)?;
		b.push(kind);
		blob(&mut b, &self.response)?;
		wrap(b"VISOUT02", &b, OUTCOME_MAX)
	}
	pub fn decode(bytes: &[u8]) -> Result2<Self> {
		let mut r = open(bytes, b"VISOUT02", OUTCOME_MAX)?;
		let generation = r.array()?;
		let cluster = r.array()?;
		let id = identity(&mut r)?;
		let claim = r.array()?;
		let decision = r.log()?;
		let result = r.blob(COMMAND_BYTES2 + 48)?.to_vec();
		let kind = r.u8()?;
		let response = r.blob(65536)?.to_vec();
		r.end()?;
		let v = Self {
			generation,
			cluster,
			identity: id,
			claim,
			decision,
			result,
			response,
		};
		check(v.terminal()?.0 == kind)?;
		Ok(v)
	}
}
/// Recomputes opaque child metadata binding; does not certify data durability or peer identity.
pub fn child_binding2(
	claim: &ClaimProjection2,
	cluster: [u8; 16],
	reservation: Log2,
	child: u32,
) -> Result2<Digest2> {
	check_claim(claim)?;
	nonzero16(cluster)?;
	let ch = claim
		.manifest
		.children
		.get(child as usize)
		.ok_or(RootError2::Codec)?;
	let mut b = b"VIS-CHILD-DURABLE-2\0".to_vec();
	b.extend(claim.manifest.identity.generation);
	b.extend(cluster);
	put_id(&mut b, claim.manifest.identity)?;
	b.extend(claim.digest()?);
	log(&mut b, reservation)?;
	b.extend(child.to_le_bytes());
	b.extend((ch.versions.len() as u32).to_le_bytes());
	for (i, v) in ch.versions.iter().enumerate() {
		let stamp = claim
			.stamps
			.iter()
			.find(|s| s.child == child && s.row as usize == i)
			.ok_or(RootError2::Codec)?;
		b.extend((i as u32).to_le_bytes());
		stamp.hlc.write(&mut b)?;
		b.extend(v.key_token);
		b.extend(v.content_commitment);
		b.push(v.tombstone as u8);
	}
	Ok(sha(&b))
}
