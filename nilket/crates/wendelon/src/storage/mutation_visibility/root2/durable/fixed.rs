use super::*;
#[derive(Clone, PartialEq, Eq)]
pub struct UnverifiedIndex2 {
	pub identity: MutationIdentity,
	pub child: u32,
	pub row: u32,
	pub hlc: Hlc2,
	pub claim: Digest2,
	pub reservation: Log2,
	pub binding: Digest2,
	pub object: [u8; 16],
	pub object_digest: Digest2,
	pub offset: u64,
	pub length: u32,
	pub physical_row: u32,
	pub tombstone: bool,
	pub partition: Option<u64>,
}
#[derive(Clone, PartialEq, Eq)]
pub struct UnverifiedReceipt2 {
	pub generation: [u8; 16],
	pub cluster: [u8; 16],
	pub identity: MutationIdentity,
	pub claim: Digest2,
	pub reservation: Log2,
	pub child: u32,
	pub binding: Digest2,
	pub original: GrantRef2,
	pub placement: Digest2,
	pub node: NodeRef2,
	pub authority: AuthorityRef2,
}
#[derive(Clone, PartialEq, Eq)]
pub struct UnverifiedGrantArchive2 {
	pub generation: [u8; 16],
	pub cluster: [u8; 16],
	pub bootstrap: Log2,
	pub applied: Log2,
	pub entry_context: Digest2,
	pub command: Command2,
	pub original_digest: Digest2,
	pub reference_context: Digest2,
}
opaque!(
	UnverifiedIndex2,
	UnverifiedReceipt2,
	UnverifiedGrantArchive2
);
impl UnverifiedIndex2 {
	/// Checks original coordinates against the declared bundle, never a physical header clock.
	/// This is structural consistency only; the bundle must still pass scoped content verification.
	/// The opaque child binding is verifier-deferred: this check does not recompute it.
	/// Later authenticated integration must compare it with child_binding2 before granting trust.
	pub fn check_original_binding(&self, bundle: &UnverifiedBundle2) -> Result2<()> {
		self.validate()?;
		bundle.validate()?;
		let (parent, (c, reservation)) = bundle
			.claims
			.iter()
			.enumerate()
			.find(|(_, (c, _))| c.manifest.identity == self.identity)
			.ok_or(RootError2::Codec)?;
		let child = c
			.manifest
			.children
			.get(self.child as usize)
			.ok_or(RootError2::Codec)?;
		let version = child
			.versions
			.get(self.row as usize)
			.ok_or(RootError2::Codec)?;
		let stamp = c
			.stamps
			.iter()
			.find(|s| s.child == self.child && s.row == self.row)
			.ok_or(RootError2::Codec)?;
		check(
			c.digest()? == self.claim
				&& *reservation == self.reservation
				&& stamp.hlc == self.hlc
				&& version.tombstone == self.tombstone
				&& child.partition == self.partition,
		)?;
		let reference = bundle
			.provenance
			.references()
			.get(self.physical_row as usize)
			.ok_or(RootError2::Codec)?;
		check(
			reference.parent as usize == parent
				&& reference.child == self.child
				&& reference.row == self.row,
		)
	}

	fn validate(&self) -> Result2<()> {
		self.identity.validate().map_err(|_| RootError2::Codec)?;
		self.hlc.bytes()?;
		crate::storage::mutation_visibility::root2::codec::valid_log(self.reservation)?;
		nonzero16(self.object)?;
		check(
			self.child < 256
				&& self.row < 512
				&& self.physical_row < 512
				&& self.length > 0
				&& (self.length as usize) <= DATA_MAX,
		)?;
		self.offset
			.checked_add(self.length as u64)
			.ok_or(RootError2::Overflow)?;
		check(self.partition != Some(0))
	}
	pub fn key(&self) -> Result2<[u8; 109]> {
		self.validate()?;
		let mut b = Vec::new();
		put_id(&mut b, self.identity)?;
		b.extend(self.child.to_le_bytes());
		b.extend(self.row.to_le_bytes());
		b.extend(self.object);
		b.extend(self.offset.to_le_bytes());
		b.extend(self.physical_row.to_le_bytes());
		b.try_into().map_err(|_| RootError2::Codec)
	}
	pub fn check_locator(&self, key: &[u8], extent_length: u64) -> Result2<()> {
		check(key == self.key()?)?;
		check(
			self.offset
				.checked_add(self.length as u64)
				.ok_or(RootError2::Overflow)?
				<= extent_length,
		)
	}
	pub fn encode(&self) -> Result2<Vec<u8>> {
		self.validate()?;
		let mut b = Vec::new();
		put_id(&mut b, self.identity)?;
		b.extend(self.child.to_le_bytes());
		b.extend(self.row.to_le_bytes());
		self.hlc.write(&mut b)?;
		b.extend(self.claim);
		log(&mut b, self.reservation)?;
		b.extend(self.binding);
		b.extend(self.object);
		b.extend(self.object_digest);
		b.extend(self.offset.to_le_bytes());
		b.extend(self.length.to_le_bytes());
		b.extend(self.physical_row.to_le_bytes());
		b.extend([self.tombstone as u8, self.partition.is_some() as u8]);
		b.extend(self.partition.unwrap_or(0).to_le_bytes());
		wrap(b"VISIDX02", &b, 4096)
	}
	pub fn decode(bytes: &[u8]) -> Result2<Self> {
		let mut r = open(bytes, b"VISIDX02", 4096)?;
		let id = identity(&mut r)?;
		let child = r.u32()?;
		let row = r.u32()?;
		let hlc = Hlc2::read(&mut r)?;
		let claim = r.array()?;
		let reservation = r.log()?;
		let binding = r.array()?;
		let object = r.array()?;
		let object_digest = r.array()?;
		let offset = r.u64()?;
		let length = r.u32()?;
		let physical_row = r.u32()?;
		let tombstone = match r.u8()? {
			0 => false,
			1 => true,
			_ => return Err(RootError2::Codec),
		};
		let placement = r.u8()?;
		let part = r.u64()?;
		let partition = match (placement, part) {
			(0, 0) => None,
			(1, n) if n > 0 => Some(n),
			_ => return Err(RootError2::Codec),
		};
		r.end()?;
		let v = Self {
			identity: id,
			child,
			row,
			hlc,
			claim,
			reservation,
			binding,
			object,
			object_digest,
			offset,
			length,
			physical_row,
			tombstone,
			partition,
		};
		v.validate()?;
		Ok(v)
	}
}
impl UnverifiedReceipt2 {
	pub fn encode(&self) -> Result2<Vec<u8>> {
		nonzero16(self.generation)?;
		nonzero16(self.cluster)?;
		check(self.identity.generation == self.generation && self.child < 256)?;
		let mut b = Vec::new();
		b.extend(self.generation);
		b.extend(self.cluster);
		put_id(&mut b, self.identity)?;
		b.extend(self.claim);
		log(&mut b, self.reservation)?;
		b.extend(self.child.to_le_bytes());
		b.extend(self.binding);
		self.original.write(&mut b)?;
		b.extend(self.placement);
		self.node.write(&mut b)?;
		self.authority.write(&mut b)?;
		b.push(1);
		wrap(b"VISRCP02", &b, 4096)
	}
	pub fn decode(bytes: &[u8]) -> Result2<Self> {
		let mut r = open(bytes, b"VISRCP02", 4096)?;
		let v = Self {
			generation: r.array()?,
			cluster: r.array()?,
			identity: identity(&mut r)?,
			claim: r.array()?,
			reservation: r.log()?,
			child: r.u32()?,
			binding: r.array()?,
			original: GrantRef2::read(&mut r)?,
			placement: r.array()?,
			node: NodeRef2::read(&mut r)?,
			authority: AuthorityRef2::read(&mut r)?,
		};
		check(r.u8()? == 1)?;
		r.end()?;
		v.encode()?;
		Ok(v)
	}
}
impl UnverifiedGrantArchive2 {
	fn validate(&self) -> Result2<()> {
		nonzero16(self.generation)?;
		nonzero16(self.cluster)?;
		crate::storage::mutation_visibility::root2::codec::valid_log(self.bootstrap)?;
		crate::storage::mutation_visibility::root2::codec::valid_log(self.applied)?;
		check(self.bootstrap.index <= self.applied.index)?;
		let Command2::Grant {
			lineage,
			incarnation,
			lease,
			predecessor,
		} = &self.command
		else {
			return Err(RootError2::Codec);
		};
		let original = OriginalGrant2 {
			lineage: *lineage,
			incarnation: *incarnation,
			lease: lease.clone(),
			log: self.applied,
			predecessor: *predecessor,
		};
		check(original.reference()?.digest == self.original_digest)
	}
	pub fn encode(&self) -> Result2<Vec<u8>> {
		self.validate()?;
		let mut b = Vec::new();
		b.extend(self.generation);
		b.extend(self.cluster);
		log(&mut b, self.bootstrap)?;
		log(&mut b, self.applied)?;
		b.extend(self.entry_context);
		let command = self.command.encode(self.generation, self.cluster)?;
		check(command.len() == 318)?;
		blob(&mut b, &command)?;
		b.extend(self.original_digest);
		b.extend(self.reference_context);
		wrap(b"VISGRP02", &b, 538)
	}
	pub fn decode(bytes: &[u8]) -> Result2<Self> {
		let mut r = open(bytes, b"VISGRP02", 538)?;
		let generation = r.array()?;
		let cluster = r.array()?;
		let bootstrap = r.log()?;
		let applied = r.log()?;
		let entry_context = r.array()?;
		let command_bytes = r.blob(318)?;
		check(command_bytes.len() == 318)?;
		let (g, c, command) = Command2::decode(command_bytes)?;
		check((g, c) == (generation, cluster))?;
		let v = Self {
			generation,
			cluster,
			bootstrap,
			applied,
			entry_context,
			command,
			original_digest: r.array()?,
			reference_context: r.array()?,
		};
		r.end()?;
		v.validate()?;
		Ok(v)
	}
}
