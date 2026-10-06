use super::super::{MutationIdentity, ProofKeyBinding};
use super::{codec::*, types::*, validation::*};

#[derive(Clone, PartialEq, Eq)]
pub enum Command2 {
	Bootstrap {
		epoch: u64,
		control: Digest2,
		length: u64,
	},
	Cover {
		kind: u8,
		external: Digest2,
	},
	EnrollSelector(ProofKeyBinding),
	EnrollAuthority {
		authority: AuthorityRef2,
		node: NodeRef2,
		key: Digest2,
	},
	RevokeAuthority(AuthorityRef2),
	Grant {
		lineage: [u8; 16],
		incarnation: [u8; 16],
		lease: SignedLease2,
		predecessor: Option<GrantRef2>,
	},
	Release {
		lineage: [u8; 16],
		latest: GrantRef2,
	},
	Cut {
		expected: u64,
		next: u64,
		configuration: Digest2,
	},
	Reserve(ClaimProjection2),
	Finalize {
		identity: MutationIdentity,
		digest: Digest2,
		reservation: Log2,
		attestation: HolderAttestation2,
	},
	Resolve {
		identity: MutationIdentity,
		digest: Digest2,
		reservation: Log2,
		cause: u8,
		anchor: Log2,
	},
}
impl std::fmt::Debug for Command2 {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("Command2 { redacted }")
	}
}
impl Command2 {
	pub fn payload(&self, generation: [u8; 16], cluster: [u8; 16]) -> Result2<Vec<u8>> {
		nonzero(generation)?;
		nonzero(cluster)?;
		let mut b = Vec::new();
		b.extend(generation);
		b.extend(cluster);
		self.write(&mut b)?;
		if b.len() > COMMAND_BYTES2 {
			return Err(RootError2::Limit);
		}
		Ok(b)
	}
	pub fn encode(&self, generation: [u8; 16], cluster: [u8; 16]) -> Result2<Vec<u8>> {
		envelope(
			b"VISEVT02",
			&self.payload(generation, cluster)?,
			COMMAND_BYTES2,
		)
	}
	pub fn decode(bytes: &[u8]) -> Result2<([u8; 16], [u8; 16], Self)> {
		let mut r = Read2::new(open(bytes, b"VISEVT02", COMMAND_BYTES2)?);
		let generation = r.array()?;
		let cluster = r.array()?;
		nonzero(generation)?;
		nonzero(cluster)?;
		let command = Self::read(&mut r)?;
		r.end()?;
		Ok((generation, cluster, command))
	}
	pub fn digest(&self, g: [u8; 16], c: [u8; 16]) -> Result2<Digest2> {
		Ok(hash(b"VISROOT2-COMMAND\0", &self.payload(g, c)?))
	}
}
impl Wire2 for Command2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		match self {
			Self::Bootstrap {
				epoch,
				control,
				length,
			} => {
				if *epoch == 0 {
					return Err(RootError2::Codec);
				}
				b.push(0);
				b.extend(epoch.to_le_bytes());
				b.extend(control);
				b.extend(length.to_le_bytes())
			}
			Self::Cover { kind, external } => {
				if *kind > 2 {
					return Err(RootError2::Codec);
				}
				b.extend([1, *kind]);
				b.extend(external)
			}
			Self::EnrollSelector(x) => {
				b.push(2);
				b.extend(x.encode().map_err(|_| RootError2::Codec)?)
			}
			Self::EnrollAuthority {
				authority,
				node,
				key,
			} => {
				b.push(3);
				authority.write(b)?;
				node.write(b)?;
				b.extend(key)
			}
			Self::RevokeAuthority(x) => {
				b.push(4);
				x.write(b)?
			}
			Self::Grant {
				lineage,
				incarnation,
				lease,
				predecessor,
			} => {
				nonzero(*lineage)?;
				nonzero(*incarnation)?;
				b.push(5);
				b.extend(lineage);
				b.extend(incarnation);
				lease.write(b)?;
				b.push(u8::from(predecessor.is_some()));
				if let Some(x) = predecessor {
					x.write(b)?
				} else {
					b.extend([0; 68])
				}
			}
			Self::Release { lineage, latest } => {
				nonzero(*lineage)?;
				b.push(6);
				b.extend(lineage);
				latest.write(b)?
			}
			Self::Cut {
				expected,
				next,
				configuration,
			} => {
				b.push(7);
				b.extend(expected.to_le_bytes());
				b.extend(next.to_le_bytes());
				b.extend(configuration)
			}
			Self::Reserve(x) => {
				b.push(8);
				blob(b, &x.encode()?)?
			}
			Self::Finalize {
				identity,
				digest,
				reservation,
				attestation,
			} => {
				b.push(9);
				b.extend(identity.encode().map_err(|_| RootError2::Codec)?);
				b.extend(digest);
				log(b, *reservation)?;
				blob(b, &attestation.encode()?)?
			}
			Self::Resolve {
				identity,
				digest,
				reservation,
				cause,
				anchor,
			} => {
				if !(1..=4).contains(cause) {
					return Err(RootError2::Codec);
				}
				b.push(10);
				b.extend(identity.encode().map_err(|_| RootError2::Codec)?);
				b.extend(digest);
				log(b, *reservation)?;
				b.push(*cause);
				log(b, *anchor)?
			}
		}
		Ok(())
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		Ok(match r.u8()? {
			0 => {
				let epoch = r.u64()?;
				if epoch == 0 {
					return Err(RootError2::Codec);
				}
				Self::Bootstrap {
					epoch,
					control: r.array()?,
					length: r.u64()?,
				}
			}
			1 => {
				let kind = r.u8()?;
				if kind > 2 {
					return Err(RootError2::Codec);
				}
				Self::Cover {
					kind,
					external: r.array()?,
				}
			}
			2 => Self::EnrollSelector(read_binding2(r)?),
			3 => Self::EnrollAuthority {
				authority: AuthorityRef2::read(r)?,
				node: NodeRef2::read(r)?,
				key: r.array()?,
			},
			4 => Self::RevokeAuthority(AuthorityRef2::read(r)?),
			5 => {
				let lineage = r.array()?;
				let incarnation = r.array()?;
				nonzero(lineage)?;
				nonzero(incarnation)?;
				let lease = SignedLease2::read(r)?;
				let flag = r.u8()?;
				let bytes = r.take(68)?;
				let predecessor = match flag {
					0 if bytes == [0; 68] => None,
					1 => Some(GrantRef2::decode(bytes)?),
					_ => return Err(RootError2::Codec),
				};
				Self::Grant {
					lineage,
					incarnation,
					lease,
					predecessor,
				}
			}
			6 => {
				let lineage = r.array()?;
				nonzero(lineage)?;
				Self::Release {
					lineage,
					latest: GrantRef2::read(r)?,
				}
			}
			7 => Self::Cut {
				expected: r.u64()?,
				next: r.u64()?,
				configuration: r.array()?,
			},
			8 => Self::Reserve(ClaimProjection2::decode(r.blob(CLAIM_BYTES2)?)?),
			9 => Self::Finalize {
				identity: MutationIdentity::decode(r.take(73)?).map_err(|_| RootError2::Codec)?,
				digest: r.array()?,
				reservation: r.log()?,
				attestation: HolderAttestation2::decode(r.blob(ATTESTATION_BYTES2)?)?,
			},
			10 => {
				let identity =
					MutationIdentity::decode(r.take(73)?).map_err(|_| RootError2::Codec)?;
				let digest = r.array()?;
				let reservation = r.log()?;
				let cause = r.u8()?;
				if !(1..=4).contains(&cause) {
					return Err(RootError2::Codec);
				}
				Self::Resolve {
					identity,
					digest,
					reservation,
					cause,
					anchor: r.log()?,
				}
			}
			_ => return Err(RootError2::Codec),
		})
	}
}
#[derive(Clone, PartialEq, Eq)]
pub enum ApplyResult2 {
	Covered,
	AlreadyProcessed,
	Registered(Log2),
	Granted(LineageRecord2),
	Closed {
		lineage: [u8; 16],
		cause: u8,
		first: Log2,
	},
	Parent {
		record: ParentRecord2,
		new_effect: bool,
	},
	Denied {
		reason: u16,
		reference: Option<Log2>,
	},
	PendingEvidence(u8),
	RejectedEvidence(u8),
}
impl std::fmt::Debug for ApplyResult2 {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("ApplyResult2 { redacted }")
	}
}
impl ApplyResult2 {
	pub fn encode(&self, g: [u8; 16], c: [u8; 16], processed: Log2) -> Result2<Vec<u8>> {
		nonzero(g)?;
		nonzero(c)?;
		let mut b = Vec::new();
		b.extend(g);
		b.extend(c);
		log(&mut b, processed)?;
		self.write(&mut b)?;
		envelope(b"VISRES02", &b, COMMAND_BYTES2)
	}
	pub fn decode(b: &[u8]) -> Result2<([u8; 16], [u8; 16], Log2, Self)> {
		let mut r = Read2::new(open(b, b"VISRES02", COMMAND_BYTES2)?);
		let g = r.array()?;
		let c = r.array()?;
		nonzero(g)?;
		nonzero(c)?;
		let l = r.log()?;
		let v = Self::read(&mut r)?;
		r.end()?;
		Ok((g, c, l, v))
	}
}
impl Wire2 for ApplyResult2 {
	fn write(&self, b: &mut Vec<u8>) -> Result2<()> {
		match self {
			Self::Covered => b.push(0),
			Self::AlreadyProcessed => b.push(1),
			Self::Registered(l) => {
				b.push(2);
				log(b, *l)?
			}
			Self::Granted(g) => {
				b.push(3);
				blob(b, &g.encode()?)?
			}
			Self::Closed {
				lineage,
				cause,
				first,
			} => {
				nonzero(*lineage)?;
				if !(1..=3).contains(cause) {
					return Err(RootError2::Codec);
				}
				b.push(4);
				b.extend(lineage);
				b.push(*cause);
				log(b, *first)?
			}
			Self::Parent { record, new_effect } => {
				b.push(5);
				blob(b, &record.encode()?)?;
				b.push(u8::from(*new_effect));
				if *new_effect {
					let Some(Terminal2::Accepted { first, .. }) = record.terminal else {
						return Err(RootError2::Codec);
					};
					log(b, first)?;
					b.extend(record.claim.accounting_delta.to_le_bytes())
				} else {
					b.extend([0; 28])
				}
			}
			Self::Denied { reason, reference } => {
				if !(1..=17).contains(reason) {
					return Err(RootError2::Codec);
				}
				b.push(6);
				b.extend(reason.to_le_bytes());
				optional_log(b, *reference)?
			}
			Self::PendingEvidence(n) => {
				if !(1..=2).contains(n) {
					return Err(RootError2::Codec);
				}
				b.extend([7, *n])
			}
			Self::RejectedEvidence(n) => {
				if !(1..=4).contains(n) {
					return Err(RootError2::Codec);
				}
				b.extend([8, *n])
			}
		}
		Ok(())
	}
	fn read(r: &mut Read2<'_>) -> Result2<Self> {
		Ok(match r.u8()? {
			0 => Self::Covered,
			1 => Self::AlreadyProcessed,
			2 => Self::Registered(r.log()?),
			3 => Self::Granted(LineageRecord2::decode(r.blob(294)?)?),
			4 => {
				let lineage = r.array()?;
				nonzero(lineage)?;
				let cause = r.u8()?;
				if !(1..=3).contains(&cause) {
					return Err(RootError2::Codec);
				}
				Self::Closed {
					lineage,
					cause,
					first: r.log()?,
				}
			}
			5 => {
				let record = ParentRecord2::decode(r.blob(CLAIM_BYTES2 + 107)?)?;
				let flag = r.u8()?;
				let first = r.optional_log()?;
				let delta = r.i64()?;
				let new_effect = match (flag, first, delta) {
					(0, None, 0) => false,
					(1, Some(l), d)
						if matches!(record.terminal,Some(Terminal2::Accepted{first,..}) if first==l)
							&& d == record.claim.accounting_delta =>
					{
						true
					}
					_ => return Err(RootError2::Codec),
				};
				Self::Parent { record, new_effect }
			}
			6 => {
				let reason = r.u16()?;
				if !(1..=17).contains(&reason) {
					return Err(RootError2::Codec);
				}
				Self::Denied {
					reason,
					reference: r.optional_log()?,
				}
			}
			7 => {
				let n = r.u8()?;
				if !(1..=2).contains(&n) {
					return Err(RootError2::Codec);
				}
				Self::PendingEvidence(n)
			}
			8 => {
				let n = r.u8()?;
				if !(1..=4).contains(&n) {
					return Err(RootError2::Codec);
				}
				Self::RejectedEvidence(n)
			}
			_ => return Err(RootError2::Codec),
		})
	}
}
