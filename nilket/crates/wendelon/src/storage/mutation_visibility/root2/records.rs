use super::super::ParentManifest;
use super::{codec::*, types::*, validation::*};

impl Wire2 for Hlc2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		valid_hlc(*self)?;
		b.extend(self.physical_ms.to_le_bytes());
		b.extend(self.logical.to_le_bytes());
		b.extend(self.node.to_le_bytes());
		Ok(())
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		let v = Self {
			physical_ms: r.u64()?,
			logical: r.u32()?,
			node: r.u32()?,
		};
		valid_hlc(v)?;
		Ok(v)
	}
}
impl Wire2 for NodeRef2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		valid_node(*self)?;
		b.extend(self.node.to_le_bytes());
		b.extend(self.incarnation);
		Ok(())
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		let v = Self {
			node: r.u32()?,
			incarnation: r.array()?,
		};
		valid_node(v)?;
		Ok(v)
	}
}
impl Wire2 for AuthorityRef2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		valid_authority(*self)?;
		b.extend(self.id);
		b.extend(self.revision.to_le_bytes());
		Ok(())
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		let v = Self {
			id: r.array()?,
			revision: r.u64()?,
		};
		valid_authority(v)?;
		Ok(v)
	}
}
impl Wire2 for GrantRef2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		nonzero(self.lineage)?;
		b.extend(self.lineage);
		log(b, self.log)?;
		b.extend(self.digest);
		Ok(())
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		let v = Self {
			lineage: r.array()?,
			log: r.log()?,
			digest: r.array()?,
		};
		nonzero(v.lineage)?;
		Ok(v)
	}
}
impl Wire2 for SignedLease2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		valid_lease(self)?;
		b.extend(self.epoch.to_le_bytes());
		b.extend(self.holder.to_le_bytes());
		b.extend(self.range_start.to_le_bytes());
		b.extend(self.range_end.to_le_bytes());
		self.start.write(b)?;
		self.expiry.write(b)?;
		b.extend(self.min_commit_index.to_le_bytes());
		b.extend(self.issuer.to_le_bytes());
		b.extend(self.signature);
		Ok(())
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		let v = Self {
			epoch: r.u64()?,
			holder: r.u32()?,
			range_start: r.u64()?,
			range_end: r.u64()?,
			start: Hlc2::read(r)?,
			expiry: Hlc2::read(r)?,
			min_commit_index: r.u64()?,
			issuer: r.u32()?,
			signature: r.array()?,
		};
		valid_lease(&v)?;
		Ok(v)
	}
}
impl Wire2 for OriginalGrant2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		valid_original(self)?;
		b.extend(self.lineage);
		b.extend(self.incarnation);
		self.lease.write(b)?;
		log(b, self.log)?;
		b.push(u8::from(self.predecessor.is_some()));
		if let Some(x) = self.predecessor {
			x.write(b)?
		} else {
			b.extend([0; 68])
		}
		Ok(())
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		let lineage = r.array()?;
		let incarnation = r.array()?;
		let lease = SignedLease2::read(r)?;
		let l = r.log()?;
		let flag = r.u8()?;
		let bytes = r.take(68)?;
		let predecessor = match flag {
			0 if bytes == [0; 68] => None,
			1 => Some(GrantRef2::from_bytes(bytes, 68)?),
			_ => return Err(RootError2::Codec),
		};
		let v = Self {
			lineage,
			incarnation,
			lease,
			log: l,
			predecessor,
		};
		valid_original(&v)?;
		Ok(v)
	}
}
impl OriginalGrant2 {
	pub fn reference(&self) -> Result2<GrantRef2> {
		Ok(GrantRef2 {
			lineage: self.lineage,
			log: self.log,
			digest: hash(b"VISROOT2-GRANT\0", &self.bytes()?),
		})
	}
}
impl Wire2 for LineageRecord2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		valid_lineage(self)?;
		self.latest.write(b)?;
		b.push(self.closure.map_or(0, |x| x.cause));
		optional_log(b, self.closure.map(|x| x.log))?;
		b.extend(self.closure.and_then(|x| x.successor).unwrap_or([0; 16]));
		Ok(())
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		let latest = OriginalGrant2::read(r)?;
		let tag = r.u8()?;
		let l = r.optional_log()?;
		let next = r.array()?;
		let closure = match (tag, l, next) {
			(0, None, n) if n == [0; 16] => None,
			(1 | 3, Some(l), n) if n == [0; 16] => Some(Closure2 {
				cause: tag,
				log: l,
				successor: None,
			}),
			(2, Some(l), n) if n != [0; 16] => Some(Closure2 {
				cause: 2,
				log: l,
				successor: Some(n),
			}),
			_ => return Err(RootError2::Codec),
		};
		let v = Self { latest, closure };
		valid_lineage(&v)?;
		Ok(v)
	}
}
impl Wire2 for ReplicaPlan2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		valid_plan(self)?;
		b.extend(self.child.to_le_bytes());
		b.push(1);
		b.push(self.nodes.len() as u8);
		for n in &self.nodes {
			n.write(b)?
		}
		Ok(())
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		let child = r.u32()?;
		if r.u8()? != 1 {
			return Err(RootError2::Codec);
		}
		let n = r.u8()?;
		if n != 1 && n != 3 {
			return Err(RootError2::Codec);
		}
		let mut nodes = Vec::new();
		for _ in 0..n {
			nodes.push(NodeRef2::read(r)?)
		}
		let v = Self { child, nodes };
		valid_plan(&v)?;
		Ok(v)
	}
}
impl Wire2 for VersionStamp2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		b.extend(self.child.to_le_bytes());
		b.extend(self.row.to_le_bytes());
		self.hlc.write(b)
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		Ok(Self {
			child: r.u32()?,
			row: r.u32()?,
			hlc: Hlc2::read(r)?,
		})
	}
}
impl Wire2 for ClaimProjection2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		valid_claim(self)?;
		b.extend(2u16.to_le_bytes());
		blob(b, &self.manifest.encode().map_err(|_| RootError2::Codec)?)?;
		b.extend(self.manifest_commitment);
		self.original.write(b)?;
		b.extend(self.placement);
		b.push(1);
		self.authority.write(b)?;
		b.extend((self.stamps.len() as u16).to_le_bytes());
		for s in &self.stamps {
			s.write(b)?
		}
		b.extend((self.plans.len() as u16).to_le_bytes());
		for p in &self.plans {
			blob(b, &p.bytes()?)?
		}
		b.extend(self.accounting_delta.to_le_bytes());
		b.extend(self.claim_commitment);
		b.extend(self.signature);
		Ok(())
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		if r.u16()? != 2 {
			return Err(RootError2::Codec);
		}
		let manifest = ParentManifest::decode(r.blob(65536)?).map_err(|_| RootError2::Codec)?;
		let manifest_commitment = r.array()?;
		let original = OriginalGrant2::read(r)?;
		let placement = r.array()?;
		if r.u8()? != 1 {
			return Err(RootError2::Codec);
		}
		let authority = AuthorityRef2::read(r)?;
		let n = r.u16()? as usize;
		if n > 512 {
			return Err(RootError2::Limit);
		}
		let rows = r.take(n.checked_mul(24).ok_or(RootError2::Overflow)?)?;
		let mut rr = Read2::new(rows);
		let mut stamps = Vec::new();
		for _ in 0..n {
			stamps.push(VersionStamp2::read(&mut rr)?)
		}
		let n = r.u16()? as usize;
		if n > 256 {
			return Err(RootError2::Limit);
		}
		let mut plans = Vec::new();
		for _ in 0..n {
			plans.push(ReplicaPlan2::from_bytes(r.blob(66)?, 66)?)
		}
		let v = Self {
			manifest,
			manifest_commitment,
			original,
			placement,
			authority,
			stamps,
			plans,
			accounting_delta: r.i64()?,
			claim_commitment: r.array()?,
			signature: r.array()?,
		};
		valid_claim(&v)?;
		Ok(v)
	}
}
impl ClaimProjection2 {
	pub fn digest(&self) -> Result2<Digest2> {
		Ok(hash(b"VISROOT2-CLAIMREF\0", &self.bytes()?))
	}
	pub fn held_bytes(&self) -> Result2<u64> {
		u64::try_from(
			self.bytes()?
				.len()
				.checked_add(111)
				.ok_or(RootError2::Overflow)?,
		)
		.map_err(|_| RootError2::Overflow)
	}
}
impl Wire2 for HolderAttestation2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		valid_attestation_shape(self)?;
		b.extend(1u16.to_le_bytes());
		self.grant.write(b)?;
		log(b, self.reservation)?;
		b.extend(self.claim_digest);
		self.authority.write(b)?;
		b.extend((self.children.len() as u16).to_le_bytes());
		for c in &self.children {
			b.extend(c.child.to_le_bytes());
			b.extend(c.durable_binding);
			b.push(c.nodes.len() as u8);
			for n in &c.nodes {
				n.write(b)?
			}
		}
		b.extend(self.signature);
		Ok(())
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		if r.u16()? != 1 {
			return Err(RootError2::Codec);
		}
		let grant = GrantRef2::read(r)?;
		let reservation = r.log()?;
		let claim_digest = r.array()?;
		let authority = AuthorityRef2::read(r)?;
		let n = r.u16()? as usize;
		if n > 256 {
			return Err(RootError2::Limit);
		}
		let mut children = Vec::new();
		for _ in 0..n {
			let child = r.u32()?;
			let durable_binding = r.array()?;
			let n = r.u8()?;
			if n > 3 || n == 0 {
				return Err(RootError2::Codec);
			}
			let mut nodes = Vec::new();
			for _ in 0..n {
				nodes.push(NodeRef2::read(r)?)
			}
			children.push(ChildAck2 {
				child,
				durable_binding,
				nodes,
			})
		}
		let v = Self {
			grant,
			reservation,
			claim_digest,
			authority,
			children,
			signature: r.array()?,
		};
		valid_attestation_shape(&v)?;
		Ok(v)
	}
}
impl Wire2 for ParentRecord2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		valid_parent(self)?;
		blob(b, &self.claim.bytes()?)?;
		log(b, self.reservation)?;
		b.extend(self.held_bytes.to_le_bytes());
		match self.terminal {
			None => {
				b.push(0);
				b.extend([0; 74]);
			}
			Some(Terminal2::Accepted { first, evidence }) => {
				b.push(1);
				log(b, first)?;
				b.extend([1, 0]);
				b.extend([0; 20]);
				b.extend(evidence)
			}
			Some(Terminal2::Refused {
				first,
				cause,
				anchor,
			}) => {
				b.push(1);
				log(b, first)?;
				b.extend([2, cause]);
				log(b, anchor)?;
				b.extend([0; 32])
			}
		}
		Ok(())
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		let claim = ClaimProjection2::from_bytes(r.blob(CLAIM_BYTES2)?, CLAIM_BYTES2)?;
		let reservation = r.log()?;
		let held_bytes = r.u64()?;
		let tag = r.u8()?;
		let first = r.optional_log()?;
		let decision = r.u8()?;
		let cause = r.u8()?;
		let anchor = r.optional_log()?;
		let evidence = r.array()?;
		let terminal = match (tag, first, decision, cause, anchor, evidence) {
			(0, None, 0, 0, None, n) if n == [0; 32] => None,
			(1, Some(first), 1, 0, None, evidence) => Some(Terminal2::Accepted { first, evidence }),
			(1, Some(first), 2, cause, Some(anchor), n)
				if (1..=4).contains(&cause) && n == [0; 32] =>
			{
				Some(Terminal2::Refused {
					first,
					cause,
					anchor,
				})
			}
			_ => return Err(RootError2::Codec),
		};
		let v = Self {
			claim,
			reservation,
			held_bytes,
			terminal,
		};
		valid_parent(&v)?;
		Ok(v)
	}
}
impl Wire2 for Selector2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		b.extend(self.binding.encode().map_err(|_| RootError2::Codec)?);
		log(b, self.enrolled)
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		Ok(Self {
			binding: read_binding2(r)?,
			enrolled: r.log()?,
		})
	}
}
impl Wire2 for AuthorityRegistration2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		self.authority.write(b)?;
		self.node.write(b)?;
		b.extend(self.public_key);
		log(b, self.enrolled)?;
		b.push(u8::from(self.revoked.is_some()));
		optional_log(b, self.revoked)
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		let authority = AuthorityRef2::read(r)?;
		let node = NodeRef2::read(r)?;
		let public_key = r.array()?;
		let enrolled = r.log()?;
		let flag = r.u8()?;
		let revoked = r.optional_log()?;
		if flag > 1
			|| (flag == 1) != revoked.is_some()
			|| revoked.is_some_and(|x| x.index <= enrolled.index)
		{
			return Err(RootError2::Codec);
		}
		Ok(Self {
			authority,
			node,
			public_key,
			enrolled,
			revoked,
		})
	}
}
impl Wire2 for EpochCut2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		if self.prior == 0 || self.prior.checked_add(1) != Some(self.new) {
			return Err(RootError2::Codec);
		}
		b.extend(self.prior.to_le_bytes());
		b.extend(self.new.to_le_bytes());
		log(b, self.log)?;
		b.extend(self.configuration);
		Ok(())
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		let v = Self {
			prior: r.u64()?,
			new: r.u64()?,
			log: r.log()?,
			configuration: r.array()?,
		};
		v.bytes()?;
		Ok(v)
	}
}
macro_rules! public_codec {($($t:ty=>$max:expr),*$(,)?)=>{$(impl $t{pub fn encode(&self)->Result2<Vec<u8>>{let b=self.bytes()?;if b.len()>$max {return Err(RootError2::Limit)}Ok(b)}pub fn decode(b:&[u8])->Result2<Self>{Self::from_bytes(b,$max)}})*};}
public_codec!(OriginalGrant2=>257,LineageRecord2=>294,ClaimProjection2=>CLAIM_BYTES2,HolderAttestation2=>ATTESTATION_BYTES2,ParentRecord2=>CLAIM_BYTES2+107,Selector2=>64,AuthorityRegistration2=>117,EpochCut2=>68,ReplicaPlan2=>66,SignedLease2=>136,GrantRef2=>68);
